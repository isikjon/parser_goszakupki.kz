#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ТУРБО ПАРАЛЛЕЛЬНЫЙ ПАРСЕР для www.goszakup.gov.kz
ВСЕ браузеры работают ОДНОВРЕМЕННО в HEADLESS режиме
"""

import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Set
import sqlite3
from bs4 import BeautifulSoup
import os
import sys
import threading
import tempfile
import logging
import signal
import atexit
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# Настройка детального логирования в файл
def setup_detailed_logging():
    """Настройка детального логирования в файл"""
    
    # Убираем все логи selenium и urllib3 из консоли
    logging.getLogger('selenium').setLevel(logging.CRITICAL)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)
    logging.getLogger('webdriver_manager').setLevel(logging.CRITICAL)
    
    # Создаем детальный логгер для файла
    file_logger = logging.getLogger('turbo_parser')
    file_logger.setLevel(logging.DEBUG)
    
    # Очищаем предыдущие хендлеры
    file_logger.handlers.clear()
    
    # Файловый хендлер с подробным форматом
    file_handler = logging.FileHandler('turbo_parser_detailed.log', encoding='utf-8', mode='w')
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    file_logger.addHandler(file_handler)
    
    return file_logger

# Инициализируем логгер
detailed_logger = setup_detailed_logging()

class TurboParallelParser:
    def __init__(self, total_browsers: int = 50, headless: bool = True):
        self.base_url = "https://www.goszakup.gov.kz"
        self.total_browsers = total_browsers
        self.headless = headless
        self.db_file = 'turbo_goszakup.db'
        self.total_pages = 10000
        self.records_per_page = 50
        self.break_time = 5  # Уменьшил передышку для headless
        
        # Статистика
        self.start_time = datetime.now()
        self.processed_pages = 0
        self.found_suppliers = 0
        self.detailed_suppliers = 0
        self.stats_lock = threading.Lock()
        
        # Для проверки дубликатов и утраченных записей
        self.processed_suppliers = set()
        self.duplicates_found = 0
        self.failed_pages: Set[int] = set()
        self.failed_suppliers: Set[str] = set()
        
        # Пул браузеров и процессов
        self.browser_pool = []
        self.browser_processes = []
        self.temp_dirs = []
        self.is_shutting_down = False
        
        detailed_logger.info("=== ИНИЦИАЛИЗАЦИЯ ТУРБО ПАРСЕРА ===")
        detailed_logger.info(f"Общее количество браузеров: {total_browsers}")
        detailed_logger.info(f"Headless режим: {headless}")
        
        # Регистрируем обработчики для корректного завершения
        self.setup_cleanup_handlers()
        
        self.init_database()
        self.setup_chrome_driver()
        
    def setup_cleanup_handlers(self):
        """Настройка обработчиков для корректного завершения"""
        # Обработчик сигналов
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Обработчик при выходе
        atexit.register(self.cleanup_all)
        
        detailed_logger.info("Обработчики очистки зарегистрированы")
    
    def signal_handler(self, signum, frame):
        """Обработчик сигналов завершения"""
        print(f"\n🛑 Получен сигнал {signum}. Завершение работы...")
        detailed_logger.info(f"Получен сигнал завершения: {signum}")
        self.cleanup_all()
        sys.exit(0)
    
    def cleanup_all(self):
        """Полная очистка всех ресурсов"""
        if self.is_shutting_down:
            return
        
        self.is_shutting_down = True
        print("\n🧹 Очистка ресурсов...")
        detailed_logger.info("Начало очистки ресурсов")
        
        # Закрываем браузеры
        self.close_all_browsers()
        
        # Убиваем оставшиеся Chrome процессы
        self.kill_chrome_processes()
        
        # Удаляем временные директории
        self.cleanup_temp_dirs()
        
        print("✅ Очистка завершена")
        detailed_logger.info("Очистка ресурсов завершена")
    
    def close_all_browsers(self):
        """Закрытие всех браузеров"""
        if not self.browser_pool:
            return
        
        print(f"🔒 Закрытие {len(self.browser_pool)} браузеров...")
        
        for i, browser in enumerate(self.browser_pool):
            try:
                browser.quit()
                detailed_logger.debug(f"Браузер {i+1} закрыт корректно")
            except Exception as e:
                detailed_logger.warning(f"Ошибка закрытия браузера {i+1}: {e}")
        
        self.browser_pool.clear()
    
    def kill_chrome_processes(self):
        """Принудительное завершение всех Chrome процессов"""
        print("🔨 Завершение Chrome процессов...")
        killed_count = 0
        
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                        # Проверяем, что это наш процесс (содержит user-data-dir)
                        cmdline = proc.info['cmdline'] or []
                        if any('user-data-dir' in str(arg) for arg in cmdline):
                            proc.kill()
                            killed_count += 1
                            detailed_logger.debug(f"Убит Chrome процесс: PID {proc.info['pid']}")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except Exception as e:
            detailed_logger.error(f"Ошибка завершения Chrome процессов: {e}")
        
        if killed_count > 0:
            print(f"⚰️  Завершено {killed_count} Chrome процессов")
            time.sleep(2)  # Даем время процессам завершиться
    
    def cleanup_temp_dirs(self):
        """Очистка временных директорий"""
        if not self.temp_dirs:
            return
        
        print(f"🗑️  Удаление {len(self.temp_dirs)} временных директорий...")
        
        for temp_dir in self.temp_dirs:
            try:
                import shutil
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    detailed_logger.debug(f"Удалена временная директория: {temp_dir}")
            except Exception as e:
                detailed_logger.warning(f"Ошибка удаления {temp_dir}: {e}")
        
        self.temp_dirs.clear()

    def setup_chrome_driver(self):
        """Настройка Chrome драйвера"""
        try:
            # Устанавливаем переменную окружения чтобы убрать логи
            os.environ['WDM_LOG_LEVEL'] = '0'
            
            self.chrome_driver_path = ChromeDriverManager().install()
            detailed_logger.info(f"Chrome драйвер установлен: {self.chrome_driver_path}")
            
            # Очищаем консоль от мусора
            self.clear_console()
            print("✅ Chrome драйвер установлен")
            
        except Exception as e:
            detailed_logger.error(f"Ошибка установки Chrome драйвера: {e}")
            print(f"❌ Ошибка установки Chrome драйвера: {e}")
            sys.exit(1)
    
    def clear_console(self):
        """Очистка консоли"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def init_database(self):
        """Инициализация БД"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS suppliers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                participant_number TEXT UNIQUE,
                name TEXT,
                bin TEXT,
                iin TEXT,
                rnn TEXT,
                supplier_id TEXT UNIQUE,
                detail_url TEXT,
                is_parsed BOOLEAN DEFAULT FALSE,
                is_failed BOOLEAN DEFAULT FALSE,
                retry_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS supplier_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier_id TEXT,
                section TEXT,
                field_name TEXT,
                field_value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(supplier_id, section, field_name)
            )
        ''')
        
        # Таблица неудачных попыток
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS failed_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_number INTEGER,
                supplier_id TEXT,
                error_message TEXT,
                attempt_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved BOOLEAN DEFAULT FALSE
            )
        ''')
        
        conn.commit()
        conn.close()
        
        detailed_logger.info("База данных инициализирована")
        
    def create_stealth_browser(self, browser_id: int):
        """Создание максимально скрытного браузера"""
        
        profile_dir = tempfile.mkdtemp(prefix=f"chrome_profile_{browser_id}_")
        self.temp_dirs.append(profile_dir)  # Для очистки
        
        options = Options()
        
        # HEADLESS режим для снижения нагрузки
        if self.headless:
            options.add_argument("--headless=new")  # Новый headless режим
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
        
        # ПОЛНОЕ ПОДАВЛЕНИЕ ВСЕХ ЛОГОВ
        options.add_argument("--disable-logging")
        options.add_argument("--disable-gpu-logging")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins")
        options.add_argument("--disable-images")
        options.add_argument("--silent")
        options.add_argument("--log-level=3")  # Минимальный уровень логов
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--disable-blink-features=AutomationControlled")
        
        # УБИРАЕМ DevTools и WARNING сообщения
        options.add_argument("--remote-debugging-port=0")  # Отключаем remote debugging
        options.add_argument("--disable-dev-tools")
        options.add_argument("--disable-gpu-sandbox")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-background-timer-throttling")
        options.add_argument("--disable-backgrounding-occluded-windows")
        options.add_argument("--disable-renderer-backgrounding")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-ipc-flooding-protection")
        
        # Оптимизация для headless
        if self.headless:
            options.add_argument("--disable-background-timer-throttling")
            options.add_argument("--disable-backgrounding-occluded-windows")
            options.add_argument("--disable-renderer-backgrounding")
            options.add_argument("--disable-background-networking")
            options.add_argument("--disable-ipc-flooding-protection")
        
        # Убираем DevTools сообщения
        options.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Убираем все уведомления
        prefs = {
            "profile.default_content_setting_values": {
                "notifications": 2,
                "media_stream_mic": 2,
                "media_stream_camera": 2,
                "geolocation": 2
            }
        }
        options.add_experimental_option("prefs", prefs)
        
        options.add_argument(f"--user-data-dir={profile_dir}")
        
        # Размеры окон (для headless не критично, но оставим)
        window_sizes = ["1920,1080", "1366,768", "1440,900", "1600,900", "1280,720"]
        size = window_sizes[browser_id % len(window_sizes)]
        options.add_argument(f"--window-size={size}")
        
        # User-Agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        user_agent = user_agents[browser_id % len(user_agents)]
        options.add_argument(f"--user-agent={user_agent}")
        
        try:
            # Подавляем ВСЕ ЛОГИ service
            service = Service(self.chrome_driver_path, log_path=os.devnull)
            
            # Перенаправляем stderr чтобы убрать WARNING сообщения
            original_stderr = sys.stderr
            sys.stderr = open(os.devnull, 'w')
            
            driver = webdriver.Chrome(service=service, options=options)
            
            # Возвращаем stderr
            sys.stderr.close()
            sys.stderr = original_stderr
            
            # Убираем признаки автоматизации
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Сохраняем процесс для мониторинга
            self.browser_processes.append(driver.service.process)
            
            detailed_logger.debug(f"Браузер {browser_id} создан успешно ({'headless' if self.headless else 'с интерфейсом'})")
            return driver
            
        except Exception as e:
            # Возвращаем stderr если была ошибка
            if 'original_stderr' in locals():
                try:
                    sys.stderr.close()
                except:
                    pass
                sys.stderr = original_stderr
            
            detailed_logger.error(f"Ошибка создания браузера {browser_id}: {e}")
            return None
    
    def init_browser_pool(self):
        """Создание пула браузеров ПАРАЛЛЕЛЬНО"""
        mode_text = "HEADLESS браузеров" if self.headless else "браузеров с интерфейсом"
        print(f"🚀 ПАРАЛЛЕЛЬНОЕ создание {self.total_browsers} {mode_text}...")
        
        # Создаем браузеры группами параллельно
        batch_size = 50  # По 50 браузеров одновременно
        total_batches = (self.total_browsers + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            if self.is_shutting_down:
                break
            
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, self.total_browsers)
            current_batch_size = end_idx - start_idx
            
            print(f"📦 ПАЧКА {batch_num + 1}/{total_batches}: Создание браузеров {start_idx + 1}-{end_idx}...")
            
            # Параллельное создание текущей пачки
            with ThreadPoolExecutor(max_workers=current_batch_size) as executor:
                future_to_id = {}
                
                for i in range(start_idx, end_idx):
                    future = executor.submit(self.create_stealth_browser, i)
                    future_to_id[future] = i
                
                # Собираем результаты
                batch_success = 0
                for future in as_completed(future_to_id):
                    browser_id = future_to_id[future]
                    try:
                        browser = future.result()
                        if browser:
                            self.browser_pool.append(browser)
                            batch_success += 1
                            print(f"✅ Браузер {browser_id + 1} создан")
                        else:
                            print(f"❌ Браузер {browser_id + 1} НЕ создан")
                    except Exception as e:
                        print(f"💥 Ошибка создания браузера {browser_id + 1}: {e}")
            
            print(f"🎯 Пачка {batch_num + 1} завершена: {batch_success}/{current_batch_size} браузеров")
            
            # Небольшая пауза между пачками
            if batch_num < total_batches - 1:
                print("⏳ Пауза 2 секунды между пачками...")
                time.sleep(2)
        
        print(f"🏁 ГОТОВО! Создано {len(self.browser_pool)}/{self.total_browsers} браузеров")
        detailed_logger.info(f"Создан пул из {len(self.browser_pool)} браузеров")
    
    def process_single_page(self, browser, page: int) -> List[Dict]:
        """Обработка одной страницы одним браузером"""
        try:
            url = f"{self.base_url}/ru/registry/supplierreg?count_record={self.records_per_page}&page={page}"
            
            detailed_logger.debug(f"Браузер обрабатывает страницу {page}")
            browser.get(url)
            
            # Ждем загрузки таблицы
            wait = WebDriverWait(browser, 15)
            table = wait.until(EC.presence_of_element_located((By.ID, "search-result")))
            
            # Получаем HTML и парсим
            html = browser.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            suppliers = []
            table = soup.find('table', {'id': 'search-result'})
            
            if table:
                tbody = table.find('tbody')
                if tbody:
                    for row in tbody.find_all('tr'):
                        cells = row.find_all('td')
                        if len(cells) >= 5:
                            supplier = {
                                'participant_number': cells[0].get_text(strip=True),
                                'name': cells[1].get_text(strip=True),
                                'bin': cells[2].get_text(strip=True),
                                'iin': cells[3].get_text(strip=True),
                                'rnn': cells[4].get_text(strip=True)
                            }
                            
                            # Извлекаем ID из ссылки
                            link = cells[1].find('a')
                            if link and '/show_supplier/' in link.get('href', ''):
                                supplier['supplier_id'] = link.get('href').split('/show_supplier/')[-1]
                                supplier['detail_url'] = link.get('href')
                                
                                # Проверяем на дубликаты
                                if supplier['supplier_id'] not in self.processed_suppliers:
                                    self.processed_suppliers.add(supplier['supplier_id'])
                                    suppliers.append(supplier)
                                    detailed_logger.debug(f"Найден поставщик: {supplier['supplier_id']} - {supplier['name']}")
                                else:
                                    with self.stats_lock:
                                        self.duplicates_found += 1
                                    detailed_logger.debug(f"Дубликат пропущен: {supplier['supplier_id']}")
            
            detailed_logger.info(f"Страница {page}: найдено {len(suppliers)} уникальных поставщиков")
            return suppliers
            
        except Exception as e:
            detailed_logger.error(f"Ошибка парсинга страницы {page}: {e}")
            with self.stats_lock:
                self.failed_pages.add(page)
            self.log_failed_attempt(page, None, str(e))
            return []
    
    def get_supplier_details(self, browser, supplier_id: str) -> Dict:
        """Получение деталей поставщика"""
        try:
            url = f"{self.base_url}/ru/registry/show_supplier/{supplier_id}"
            
            detailed_logger.debug(f"Получение деталей поставщика {supplier_id}")
            browser.get(url)
            
            wait = WebDriverWait(browser, 15)
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "table")))
            
            html = browser.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            details = {}
            
            # Основная таблица
            main_table = soup.find('table', class_='table table-striped')
            if main_table:
                for row in main_table.find_all('tr'):
                    cells = row.find_all(['th', 'td'])
                    if len(cells) == 2:
                        key = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        details[key] = value
            
            detailed_logger.debug(f"Получены детали поставщика {supplier_id}: {len(details)} полей")
            return details
            
        except Exception as e:
            detailed_logger.error(f"Ошибка получения деталей {supplier_id}: {e}")
            with self.stats_lock:
                self.failed_suppliers.add(supplier_id)
            self.log_failed_attempt(None, supplier_id, str(e))
            return {}
    
    def log_failed_attempt(self, page: int = None, supplier_id: str = None, error: str = ""):
        """Логирование неудачных попыток"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO failed_attempts (page_number, supplier_id, error_message)
                VALUES (?, ?, ?)
            ''', (page, supplier_id, error))
            conn.commit()
            detailed_logger.warning(f"Неудачная попытка зафиксирована: page={page}, supplier={supplier_id}")
        except Exception as e:
            detailed_logger.error(f"Ошибка логирования неудачной попытки: {e}")
        finally:
            conn.close()
    
    def save_supplier(self, supplier: dict, details: dict = None):
        """Сохранение поставщика в БД"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO suppliers 
                (participant_number, name, bin, iin, rnn, supplier_id, detail_url, is_parsed, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                supplier.get('participant_number', ''),
                supplier.get('name', ''),
                supplier.get('bin', ''),
                supplier.get('iin', ''),
                supplier.get('rnn', ''),
                supplier.get('supplier_id', ''),
                supplier.get('detail_url', ''),
                details is not None,
                datetime.now().isoformat()
            ))
            
            if details and supplier.get('supplier_id'):
                supplier_id = supplier['supplier_id']
                cursor.execute('DELETE FROM supplier_details WHERE supplier_id = ?', (supplier_id,))
                
                for field_name, field_value in details.items():
                    cursor.execute('''
                        INSERT INTO supplier_details (supplier_id, section, field_name, field_value)
                        VALUES (?, ?, ?, ?)
                    ''', (supplier_id, 'basic', field_name, str(field_value)))
                
                detailed_logger.debug(f"Поставщик {supplier_id} сохранен с деталями ({len(details)} полей)")
            else:
                detailed_logger.debug(f"Поставщик {supplier.get('supplier_id')} сохранен без деталей")
            
            conn.commit()
            
        except Exception as e:
            detailed_logger.error(f"Ошибка сохранения поставщика: {e}")
        finally:
            conn.close()
    
    def parallel_round(self, pages: List[int]) -> int:
        """Параллельный раунд обработки страниц"""
        round_start = datetime.now()
        all_suppliers = []
        
        # Создаем задачи для параллельного выполнения
        with ThreadPoolExecutor(max_workers=len(self.browser_pool)) as executor:
            # Создаем задачи - каждый браузер получает свою страницу
            future_to_page = {}
            
            for i, page in enumerate(pages[:len(self.browser_pool)]):
                browser = self.browser_pool[i]
                future = executor.submit(self.process_single_page, browser, page)
                future_to_page[future] = page
            
            # Собираем результаты
            for future in as_completed(future_to_page):
                page = future_to_page[future]
                try:
                    suppliers = future.result()
                    all_suppliers.extend(suppliers)
                    
                    # Сохраняем найденных поставщиков
                    for supplier in suppliers:
                        self.save_supplier(supplier)
                    
                    with self.stats_lock:
                        self.processed_pages += 1
                        self.found_suppliers += len(suppliers)
                    
                    detailed_logger.info(f"Страница {page} обработана: {len(suppliers)} поставщиков")
                    
                except Exception as e:
                    detailed_logger.error(f"Ошибка обработки страницы {page}: {e}")
        
        round_time = datetime.now() - round_start
        detailed_logger.info(f"Раунд завершен за {round_time.total_seconds():.1f}с: {len(all_suppliers)} поставщиков с {len(pages[:len(self.browser_pool)])} страниц")
        
        return len(all_suppliers)
    
    def print_turbo_stats(self):
        """Вывод турбо статистики"""
        with self.stats_lock:
            runtime = datetime.now() - self.start_time
            pages_percent = (self.processed_pages / self.total_pages) * 100
            
            # Очищаем консоль и выводим только статистику
            self.clear_console()
            
            mode_text = "HEADLESS" if self.headless else "С ИНТЕРФЕЙСОМ"
            print(f"🚀 ТУРБО ПАРАЛЛЕЛЬНЫЙ ПАРСЕР ({mode_text})")
            print("=" * 70)
            print(f"⏱️  Время работы: {str(runtime).split('.')[0]}")
            print(f"🌐 Активных браузеров: {len(self.browser_pool)} ({mode_text})")
            print(f"📄 Страниц: {self.processed_pages:,}/{self.total_pages:,} ({pages_percent:.1f}%)")
            print(f"👥 Найдено: {self.found_suppliers:,} поставщиков")
            print(f"🔍 Детализировано: {self.detailed_suppliers:,}")
            print(f"🔄 Дубликатов: {self.duplicates_found:,}")
            print(f"❌ Неудач страниц: {len(self.failed_pages)}")
            print(f"❌ Неудач деталей: {len(self.failed_suppliers)}")
            
            if runtime.total_seconds() > 0:
                pages_per_min = self.processed_pages / (runtime.total_seconds() / 60)
                suppliers_per_min = self.found_suppliers / (runtime.total_seconds() / 60)
                print(f"⚡ Скорость: {pages_per_min:.1f} стр/мин | {suppliers_per_min:.1f} поставщиков/мин")
            
            print("=" * 70)
            
            # Логируем в файл подробную статистику
            detailed_logger.info(f"ТУРБО СТАТИСТИКА: browsers={len(self.browser_pool)}, pages={self.processed_pages}, found={self.found_suppliers}, detailed={self.detailed_suppliers}")
    
    def run_turbo_parsing(self, start_page: int = 1, end_page: int = 10000):
        """Запуск турбо парсинга"""
        
        self.clear_console()
        mode_text = "HEADLESS" if self.headless else "С ИНТЕРФЕЙСОМ"
        print(f"🚀 ТУРБО ПАРАЛЛЕЛЬНЫЙ ПАРСЕР ({mode_text})")
        print("=" * 70)
        print(f"🎯 Настройки:")
        print(f"   • Режим: {mode_text}")
        print(f"   • Общее количество браузеров: {self.total_browsers}")
        print(f"   • Передышка между раундами: {self.break_time} секунд")
        print(f"   • Записей на странице: {self.records_per_page}")
        print(f"   • Диапазон страниц: {start_page}-{end_page}")
        print("=" * 70)
        
        detailed_logger.info("=== НАЧАЛО ТУРБО ПАРСИНГА ===")
        detailed_logger.info(f"Диапазон страниц: {start_page}-{end_page}")
        detailed_logger.info(f"Headless режим: {self.headless}")
        
        try:
            # Создаем пул браузеров
            self.init_browser_pool()
            
            if not self.browser_pool:
                print("❌ Не удалось создать браузеры!")
                return
            
            # Подготовка страниц для обработки
            all_pages = list(range(start_page, end_page + 1))
            
            # Запускаем статистику в отдельном потоке
            stats_thread = threading.Thread(target=self.stats_monitor, daemon=True)
            stats_thread.start()
            
            # Обрабатываем страницы раундами
            current_page_index = 0
            round_number = 1
            
            while current_page_index < len(all_pages) and not self.is_shutting_down:
                round_start = datetime.now()
                
                # Берем следующие N страниц (по количеству браузеров)
                pages_for_round = all_pages[current_page_index:current_page_index + len(self.browser_pool)]
                current_page_index += len(pages_for_round)
                
                print(f"\n🔥 РАУНД {round_number}: Обработка страниц {pages_for_round[0]}-{pages_for_round[-1]} ({len(pages_for_round)} браузеров)")
                
                # ВСЕ БРАУЗЕРЫ РАБОТАЮТ ОДНОВРЕМЕННО!
                suppliers_count = self.parallel_round(pages_for_round)
                
                round_time = datetime.now() - round_start
                print(f"✅ Раунд {round_number} завершен за {round_time.total_seconds():.1f}с: {suppliers_count} поставщиков")
                
                # ПЕРЕДЫШКА между раундами
                if current_page_index < len(all_pages) and not self.is_shutting_down:
                    print(f"😴 Передышка {self.break_time} секунд...")
                    time.sleep(self.break_time)
                
                round_number += 1
        
        except KeyboardInterrupt:
            print("\n⏹️  Остановка по запросу пользователя...")
            detailed_logger.info("Парсинг остановлен пользователем")
        
        except Exception as e:
            print(f"\n💥 Ошибка: {e}")
            detailed_logger.error(f"Критическая ошибка: {e}")
        
        finally:
            # Принудительная очистка
            self.cleanup_all()
        
        self.print_turbo_stats()
        print(f"\n🎉 ТУРБО ПАРСИНГ ЗАВЕРШЕН! ({'Headless' if self.headless else 'С интерфейсом'})")
        detailed_logger.info("=== ТУРБО ПАРСИНГ ЗАВЕРШЕН ===")
    
    def stats_monitor(self):
        """Мониторинг статистики каждые 5 секунд"""
        while True:
            time.sleep(5)  # ОБНОВЛЕНИЕ КАЖДЫЕ 5 СЕКУНД
            self.print_turbo_stats()

def main():
    # Убираем системные предупреждения
    import warnings
    warnings.filterwarnings("ignore")
    
    print("🚀 ТУРБО ПАРАЛЛЕЛЬНЫЙ ПАРСЕР")
    print("1. Демо HEADLESS (20 браузеров, первые 100 страниц)")
    print("2. Быстрый HEADLESS (50 браузеров, 1000 страниц)")
    print("3. Полный HEADLESS (100 браузеров, 10,000 страниц)")
    print("4. Экстрим HEADLESS (200 браузеров, 10,000 страниц)")
    print("5. С интерфейсом (10 браузеров, демо)")
    print("6. 🔧 КАСТОМНЫЕ НАСТРОЙКИ (выбираете сами)")
    
    choice = input("Выбор (1-6): ").strip()
    
    if choice == "1":
        parser = TurboParallelParser(total_browsers=20, headless=True)
        parser.run_turbo_parsing(1, 100)
    elif choice == "2":
        parser = TurboParallelParser(total_browsers=50, headless=True)
        parser.run_turbo_parsing(1, 1000)
    elif choice == "3":
        confirm = input("⚠️  100 HEADLESS браузеров! Продолжить? (y/N): ")
        if confirm.lower() == 'y':
            parser = TurboParallelParser(total_browsers=100, headless=True)
            parser.run_turbo_parsing(1, 10000)
        else:
            print("❌ Отменено")
    elif choice == "4":
        confirm = input("⚠️  200 HEADLESS браузеров! ЭКСТРИМ! Продолжить? (y/N): ")
        if confirm.lower() == 'y':
            parser = TurboParallelParser(total_browsers=200, headless=True)
            parser.run_turbo_parsing(1, 10000)
        else:
            print("❌ Отменено")
    elif choice == "5":
        parser = TurboParallelParser(total_browsers=10, headless=False)
        parser.run_turbo_parsing(1, 50)
    elif choice == "6":
        print("\n🔧 КАСТОМНЫЕ НАСТРОЙКИ")
        print("=" * 50)
        
        # Режим браузера
        print("Режим браузера:")
        print("1. HEADLESS (без интерфейса, быстрее)")
        print("2. С интерфейсом (видимые окна)")
        
        mode_choice = input("Режим (1-2): ").strip()
        headless = True if mode_choice == "1" else False
        
        # Количество браузеров
        while True:
            try:
                browsers_count = int(input("Количество браузеров (1-500): "))
                if 1 <= browsers_count <= 500:
                    break
                else:
                    print("❌ Введите число от 1 до 500")
            except ValueError:
                print("❌ Введите корректное число")
        
        # Диапазон страниц
        print("\nДиапазон страниц для парсинга:")
        print("💡 Всего страниц на сайте: ~10,000")
        print("💡 На каждой странице: ~50 поставщиков")
        
        while True:
            try:
                start_page = int(input("Начальная страница (1-10000): "))
                if 1 <= start_page <= 10000:
                    break
                else:
                    print("❌ Введите число от 1 до 10000")
            except ValueError:
                print("❌ Введите корректное число")
        
        while True:
            try:
                end_page = int(input(f"Конечная страница ({start_page}-10000): "))
                if start_page <= end_page <= 10000:
                    break
                else:
                    print(f"❌ Введите число от {start_page} до 10000")
            except ValueError:
                print("❌ Введите корректное число")
        
        # Подтверждение
        total_pages = end_page - start_page + 1
        estimated_suppliers = total_pages * 50
        mode_text = "HEADLESS" if headless else "С ИНТЕРФЕЙСОМ"
        
        print("\n📋 НАСТРОЙКИ:")
        print(f"   • Режим: {mode_text}")
        print(f"   • Браузеров: {browsers_count}")
        print(f"   • Страницы: {start_page}-{end_page} ({total_pages:,} страниц)")
        print(f"   • Примерно поставщиков: ~{estimated_suppliers:,}")
        
        if browsers_count > 100:
            print(f"⚠️  ВНИМАНИЕ: {browsers_count} браузеров - это большая нагрузка!")
        
        confirm = input("\n🚀 Начать парсинг? (y/N): ")
        
        if confirm.lower() == 'y':
            parser = TurboParallelParser(total_browsers=browsers_count, headless=headless)
            parser.run_turbo_parsing(start_page, end_page)
        else:
            print("❌ Отменено")
    else:
        print("❌ Неверный выбор")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⏹️  Парсинг остановлен пользователем")
        detailed_logger.info("Парсинг остановлен пользователем")
    except Exception as e:
        print(f"\n💥 Ошибка: {e}")
        detailed_logger.error(f"Критическая ошибка: {e}") 