import asyncio
import aiohttp
import logging
import requests
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, field
import json
import time


@dataclass
class IdoSellApiConfig:
    """Konfiguracja dla klienta IdoSell API"""
    domain: str  # np. "clientxxx.idosell.com"
    api_key: str = ""  # Opcjonalny klucz API (jeśli używany)
    batch_size: int = 100  # Maksymalnie 100 produktów na żądanie (limit IdoSell)
    timeout: int = 120  # 2 minuty - dla dużych batch'y
    max_workers: int = 1  # Limit 60 wywołań/minutę = ~1 na sekundę
    max_retries: int = 3
    retry_delay: float = 3.0  # 3 sekundy opóźnienie po błędzie
    api_version: str = "v6"
    max_request_size_mb: int = 10  # Limit 10MB na żądanie
    
    # Domyślne ustawienia dla żądania
    default_settings: Dict[str, Any] = field(default_factory=lambda: {
        "settingModificationType": "edit"  # Tryb edycji produktów
    })


class IdoSellProductClient:
    """
    Klient do wykonywania żądań PUT na endpoincie IdoSell API
    /api/admin/v6/products/products zgodnie z oficjalną dokumentacją
    """
    
    def __init__(self, config: IdoSellApiConfig):
        """
        Inicjalizuje klienta z konfiguracją IdoSell API
        
        Args:
            config: Obiekt konfiguracyjny IdoSellApiConfig
        """
        self.config = config
        self.base_url = f"https://{config.domain}/api/admin/{config.api_version}"
        self.endpoint = f"{self.base_url}/products/products"
        self.logger = logging.getLogger(__name__)
        
        # Nagłówki zgodne z dokumentacją IdoSell
        self.default_headers = {
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        
        # Dodaj klucz API do nagłówków jeśli podano
        if config.api_key:
            self.default_headers['Authorization'] = f'Bearer {config.api_key}'
    
    def validate_product_data(self, product: Dict[str, Any]) -> bool:
        """
        Waliduje dane produktu przed wysłaniem do API IdoSell
        
        Args:
            product: Słownik z danymi produktu
            
        Returns:
            True jeśli dane są prawidłowe
            
        Raises:
            ValueError: Gdy brak wymaganych pól lub nieprawidłowe dane
        """
        # productId jest wymagane dla operacji PUT (aktualizacja)
        if 'productId' not in product:
            raise ValueError("productId jest wymagane dla aktualizacji produktu")
        
        # Walidacja typów numerycznych
        numeric_fields = [
            'productRetailPrice', 'productRetailPriceNet', 'productWholesalePrice',
            'productWholesalePriceNet', 'productMinimalPrice', 'productVat', 'productWeight'
        ]
        
        for field in numeric_fields:
            if field in product and product[field] is not None:
                try:
                    float(str(product[field]))
                except (ValueError, TypeError):
                    raise TypeError(f"{field} musi być liczbą lub string reprezentującym liczbę")
        
        # Walidacja kodów języków w strukturach wielojęzycznych
        multilang_fields = [
            'productNames', 'productParamDescriptions', 'productLongDescriptions',
            'productMetaTitles', 'productMetaDescriptions', 'productMetaKeywords'
        ]
        
        for field in multilang_fields:
            if field in product and product[field]:
                lang_data_key = f"{field}LangData"
                if lang_data_key in product[field]:
                    for lang_entry in product[field][lang_data_key]:
                        if 'langId' not in lang_entry:
                            raise ValueError(f"Brak langId w {field}")
        
        return True
    
    def prepare_batch_payload(
        self, 
        products: List[Dict[str, Any]],
        custom_settings: Optional[Dict[str, Any]] = None,
        pictures_settings: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Przygotowuje payload dla żądania batch zgodnie z formatem IdoSell API
        
        Args:
            products: Lista produktów do aktualizacji
            custom_settings: Dodatkowe ustawienia (domyślnie settingModificationType=edit)
            pictures_settings: Ustawienia dla zdjęć produktów
            
        Returns:
            Słownik z kompletnym payload zgodnym z API IdoSell
        """
        # Walidacja każdego produktu
        for i, product in enumerate(products):
            try:
                self.validate_product_data(product)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Błąd walidacji produktu {i+1}: {e}")
        
        # Przygotowanie settings
        settings = self.config.default_settings.copy()
        if custom_settings:
            settings.update(custom_settings)
        
        # Struktura zgodna z dokumentacją IdoSell API
        payload = {
            "params": {
                "settings": settings,
                "products": products
            }
        }
        
        # Dodaj ustawienia zdjęć jeśli podano
        if pictures_settings:
            payload["params"]["picturesSettings"] = pictures_settings
        
        # Sprawdź rozmiar payload (limit 10MB)
        payload_size_mb = len(json.dumps(payload).encode('utf-8')) / (1024 * 1024)
        if payload_size_mb > self.config.max_request_size_mb:
            raise ValueError(
                f"Rozmiar żądania ({payload_size_mb:.2f}MB) przekracza limit "
                f"{self.config.max_request_size_mb}MB. Zmniejsz batch_size."
            )
        
        return payload
    
    async def _send_single_request(
        self, 
        session: aiohttp.ClientSession, 
        products: List[Dict[str, Any]],
        custom_settings: Optional[Dict[str, Any]] = None,
        pictures_settings: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Wysyła pojedyncze żądanie PUT batch do IdoSell API
        
        Args:
            session: Sesja aiohttp
            products: Lista produktów do aktualizacji
            custom_settings: Dodatkowe ustawienia
            pictures_settings: Ustawienia zdjęć
            
        Returns:
            Słownik z wynikiem żądania
        """
        payload = self.prepare_batch_payload(products, custom_settings, pictures_settings)
        
        for attempt in range(self.config.max_retries):
            try:
                timeout = aiohttp.ClientTimeout(total=self.config.timeout)
                async with session.put(
                    self.endpoint,
                    json=payload,
                    headers=self.default_headers,
                    timeout=timeout
                ) as response:
                    
                    result = {
                        'products': products,
                        'products_count': len(products),
                        'status_code': response.status,
                        'success': response.status in [200, 201, 207],  # 207 = Multi-Status
                        'response': None,
                        'error': None,
                        'attempt': attempt + 1,
                        'payload_size_mb': len(json.dumps(payload).encode('utf-8')) / (1024 * 1024)
                    }
                    
                    try:
                        result['response'] = await response.json()
                    except:
                        result['response'] = await response.text()
                    
                    # IdoSell może zwracać różne kody sukcesu
                    if response.status == 200:
                        self.logger.info(f"Batch {len(products)} produktów - pełny sukces")
                        return result
                    elif response.status == 207:
                        # Multi-Status - niektóre produkty OK, niektóre z błędami
                        self.logger.warning(f"Batch {len(products)} produktów - częściowy sukces (207)")
                        return result
                    elif response.status == 401:
                        result['error'] = "Błąd autoryzacji - sprawdź dane dostępowe"
                        self.logger.error(result['error'])
                        return result  # Nie ponawiaj dla błędów autoryzacji
                    elif response.status == 429:
                        # Rate limiting (60 wywołań/minutę)
                        wait_time = self.config.retry_delay * (attempt + 1)
                        result['error'] = f"Rate limiting (60/min) - czekam {wait_time}s"
                        self.logger.warning(result['error'])
                        if attempt < self.config.max_retries - 1:
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            return result
                    # Inne błędy - loguj i ewentualnie ponów próbę
                    else:
                        result['error'] = f"HTTP {response.status}: {result['response']}"
                        if attempt == self.config.max_retries - 1:
                            return result
                        
            except asyncio.TimeoutError:
                error_msg = f"Timeout po {self.config.timeout}s"
                self.logger.warning(f"Attempt {attempt + 1}: {error_msg}")
                if attempt == self.config.max_retries - 1:
                    return {
                        'products': products,
                        'products_count': len(products),
                        'status_code': 0,
                        'success': False,
                        'response': None,
                        'error': error_msg,
                        'attempt': attempt + 1
                    }
                    
            except Exception as e:
                error_msg = f"Błąd połączenia: {str(e)}"
                self.logger.error(f"Attempt {attempt + 1}: {error_msg}")
                if attempt == self.config.max_retries - 1:
                    return {
                        'products': products,
                        'products_count': len(products),
                        'status_code': 0,
                        'success': False,
                        'response': None,
                        'error': error_msg,
                        'attempt': attempt + 1
                    }
            
            # Opóźnienie przed kolejną próbą
            if attempt < self.config.max_retries - 1:
                await asyncio.sleep(self.config.retry_delay * (attempt + 1))
    
    async def _send_batch(
        self, 
        batch: List[Dict[str, Any]],
        custom_settings: Optional[Dict[str, Any]] = None,
        pictures_settings: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Wysyła jeden batch produktów do IdoSell API
        """
        connector = aiohttp.TCPConnector(limit=self.config.max_workers)
        async with aiohttp.ClientSession(connector=connector) as session:
            result = await self._send_single_request(
                session, batch, custom_settings, pictures_settings
            )
            return [result]
    
    def _create_batches(
        self, 
        products_list: List[Dict[str, Any]]
    ) -> List[List[Dict[str, Any]]]:
        """
        Dzieli produkty na batche zgodnie z limitami IdoSell API (max 100/batch, 10MB)
        """
        batches = []
        current_batch = []
        current_size = 0
        
        for product in products_list:
            # Szacunkowy rozmiar produktu w JSON
            product_size = len(json.dumps(product).encode('utf-8'))
            
            # Sprawdź czy dodanie produktu nie przekroczy limitów
            if (len(current_batch) >= self.config.batch_size or 
                (current_size + product_size) > (self.config.max_request_size_mb * 1024 * 1024 * 0.8)):
                
                if current_batch:
                    batches.append(current_batch)
                    current_batch = [product]
                    current_size = product_size
                else:
                    # Pojedynczy produkt za duży
                    self.logger.warning(f"Produkt {product.get('productId', 'unknown')} może być za duży")
                    current_batch = [product]
                    current_size = product_size
            else:
                current_batch.append(product)
                current_size += product_size
        
        if current_batch:
            batches.append(current_batch)
        
        return batches
    
    async def send_requests_async(
        self, 
        products_list: List[Dict[str, Any]],
        progress_callback: Optional[Callable[[int, int], None]] = None,
        custom_settings: Optional[Dict[str, Any]] = None,
        pictures_settings: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Wysyła żądania aktualizacji produktów asynchronicznie w batch'ach
        """
        batches = self._create_batches(products_list)
        all_results = []
        processed_products = 0
        
        self.logger.info(
            f"🚀 Rozpoczynam aktualizację {len(products_list)} produktów "
            f"w {len(batches)} batch'ach (limit: {self.config.batch_size}/batch, "
            f"rate limit: 60 wywołań/min)"
        )
        
        for i, batch in enumerate(batches):
            start_time = time.time()
            
            # Opóźnienie między batch'ami żeby nie przekroczyć 60/min
            if i > 0:
                # Minimum 1 sekunda między żądaniami
                await asyncio.sleep(1.2)
            
            batch_results = await self._send_batch(batch, custom_settings, pictures_settings)
            end_time = time.time()
            
            all_results.extend(batch_results)
            processed_products += len(batch)
            
            # Wywołaj callback postępu
            if progress_callback:
                progress_callback(processed_products, len(products_list))
            
            # Logowanie postępu batch'a
            batch_result = batch_results[0]
            success = batch_result['success']
            products_in_batch = batch_result.get('products_count', len(batch))
            payload_size = batch_result.get('payload_size_mb', 0)
            
            if success:
                if batch_result['status_code'] == 207:
                    status_msg = f"⚠️ CZĘŚCIOWY SUKCES (kod 207) - sprawdź szczegóły"
                else:
                    status_msg = f"✅ PEŁNY SUKCES (kod {batch_result['status_code']})"
            else:
                status_msg = f"❌ BŁĄD: {batch_result['error']}"
            
            self.logger.info(
                f"Batch {i+1}/{len(batches)}: {products_in_batch} produktów "
                f"({payload_size:.2f}MB) - {status_msg}, czas: {end_time - start_time:.2f}s"
            )
        
        return all_results
    
    def send_requests_sync(
        self, 
        products_list: List[Dict[str, Any]],
        progress_callback: Optional[Callable[[int, int], None]] = None,
        custom_settings: Optional[Dict[str, Any]] = None,
        pictures_settings: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Synchroniczna wersja wysyłania żądań aktualizacji produktów
        """
        return asyncio.run(
            self.send_requests_async(
                products_list, progress_callback, custom_settings, pictures_settings
            )
        )
    
    def get_summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Tworzy podsumowanie wyników aktualizacji produktów
        """
        total_batches = len(results)
        successful_batches = sum(1 for r in results if r['success'])
        partial_success_batches = sum(1 for r in results if r['success'] and r['status_code'] == 207)
        full_success_batches = sum(1 for r in results if r['success'] and r['status_code'] == 200)
        failed_batches = total_batches - successful_batches
        
        # Zlicz łączną liczbę produktów
        total_products = sum(r.get('products_count', 0) for r in results)
        successful_products = sum(r.get('products_count', 0) for r in results if r['success'])
        failed_products = total_products - successful_products
        
        # Analiza typów błędów
        error_types = {}
        for result in results:
            if not result['success'] and result['error']:
                error_key = result['error'].split(':')[0]  # Pierwsza część błędu
                error_types[error_key] = error_types.get(error_key, 0) + 1
        
        return {
            'total_batches': total_batches,
            'full_success_batches': full_success_batches,
            'partial_success_batches': partial_success_batches,
            'failed_batches': failed_batches,
            'total': total_products,
            'success': successful_products,
            'failed': failed_products,
            'success_rate': (successful_products / total_products * 100) if total_products > 0 else 0,
            'batch_success_rate': (successful_batches / total_batches * 100) if total_batches > 0 else 0,
            'error_types': error_types,
            'has_partial_success': partial_success_batches > 0
        }
    
    def create_product_template(self) -> Dict[str, Any]:
        """
        Tworzy szablon produktu z najważniejszymi polami zgodnie z API IdoSell
        
        Returns:
            Słownik z szablonem produktu
        """
        return {
            # WYMAGANE dla aktualizacji
            "productId": "",  # ID produktu do aktualizacji
            
            # PODSTAWOWE INFORMACJE
            "productDisplayedCode": "",  # Kod SKU produktu
            "productRetailPrice": "0.00",  # Cena detaliczna
            "productVat": "23.00",  # VAT w procentach
            
            # OPISY WIELOJĘZYCZNE
            "productNames": {
                "productNamesLangData": [
                    {
                        "langId": "pol",  # pl, en, de, itp.
                        "productName": "",
                        "shopId": "1"
                    }
                ]
            },
            "productParamDescriptions": {
                "productParamDescriptionsLangData": [
                    {
                        "langId": "pol",
                        "productParamDescriptions": "",
                        "shopId": "1"
                    }
                ]
            },
            "productLongDescriptions": {
                "productLongDescriptionsLangData": [
                    {
                        "langId": "pol",
                        "productLongDescription": "",
                        "shopId": "1"
                    }
                ]
            },
            
            # KATEGORIE I PRODUCENCI
            "categoryId": "1",
            "categoryName": "",
            "producerId": "1",
            "producerName": "",
            
            # DODATKOWE OPCJONALNE
            "productNote": "",
            "productWeight": "1",
            "countryOfOrigin": "PL",
            "shopsMask": "1"
        }


# Przykład użycia zgodny z rzeczywistą dokumentacją IdoSell
if __name__ == "__main__":
    # Konfiguracja dla IdoSell API
    config = IdoSellApiConfig(
        domain="clientxxx.idosell.com",  # Zastąp swoją domeną
        api_key="",  # Dodaj klucz API jeśli wymagany
        batch_size=50,  # Bezpieczny rozmiar batch'a
        timeout=120,  # 2 minuty na żądanie
        max_workers=1,  # Jeden worker ze względu na rate limiting
        max_retries=3
    )
    
    # Utworzenie klienta
    client = IdoSellProductClient(config)
    
    # Przykładowe produkty zgodne z API IdoSell
    products_data = [
        {
            "productId": "101",
            "productDisplayedCode": "TEST-001",
            "productRetailPrice": "199.99",
            "productVat": "23.00",
            "productNote": "Testowy produkt aktualizowany przez API",
            "categoryId": "1",
            "categoryName": "Kategoria testowa",
            "producerId": "1", 
            "producerName": "Producent testowy",
            "countryOfOrigin": "PL",
            "productNames": {
                "productNamesLangData": [
                    {
                        "langId": "pol",
                        "productName": "Testowy produkt PL",
                        "shopId": "1"
                    },
                    {
                        "langId": "eng",
                        "productName": "Test product EN",
                        "shopId": "1"
                    }
                ]
            },
            "productParamDescriptions": {
                "productParamDescriptionsLangData": [
                    {
                        "langId": "pol",
                        "productParamDescriptions": "Krótki opis produktu po polsku",
                        "shopId": "1"
                    }
                ]
            }
        }
        # Dodaj więcej produktów...
    ]
    
    # Callback dla śledzenia postępu
    def progress_callback(processed, total):
        progress = (processed / total) * 100
        print(f"📊 Postęp: {processed}/{total} produktów ({progress:.1f}%)")
    
    # Dodatkowe ustawienia (opcjonalne)
    custom_settings = {
        "settingModificationType": "edit",
        "settingPriceFormat": "gross",  # brutto/netto
        "settingAddingCategoryAllowed": "1"
    }
    
    try:
        print("🔄 Uruchamiam aktualizację produktów w IdoSell...")
        
        results = client.send_requests_sync(
            products_data,
            progress_callback=progress_callback,
            custom_settings=custom_settings
        )
        
        # Szczegółowe podsumowanie
        summary = client.get_summary(results)
        print("\n" + "="*50)
        print("📈 PODSUMOWANIE AKTUALIZACJI IDOSELL")
        print("="*50)
        print(f"📦 Łącznie produktów: {summary['total']}")
        print(f"✅ Pomyślnie zaktualizowane: {summary['success']}")
        print(f"❌ Błędy: {summary['failed']}")
        print(f"📊 Wskaźnik sukcesu: {summary['success_rate']:.1f}%")
        print(f"🔄 Batch'e - sukces: {summary['full_success_batches']}, "
              f"częściowy: {summary['partial_success_batches']}, "
              f"błędy: {summary['failed_batches']}")
        
        if summary['has_partial_success']:
            print("⚠️ UWAGA: Niektóre batch'e zwróciły kod 207 (częściowy sukces)")
            print("   Sprawdź szczegóły w response dla każdego batch'a")
        
        if summary['error_types']:
            print(f"\n🔍 Typy błędów:")
            for error, count in summary['error_types'].items():
                print(f"   - {error}: {count} batch'ów")
        
        # Pokaż szczegóły pierwszych błędów
        failed_batches = [r for r in results if not r['success']]
        if failed_batches:
            print(f"\n💥 Szczegóły błędów (pierwsze {min(3, len(failed_batches))} batch'e):")
            for i, result in enumerate(failed_batches[:3]):
                print(f"   {i+1}. Batch {result.get('products_count', '?')} produktów:")
                print(f"      Error: {result['error']}")
                print(f"      HTTP: {result['status_code']}")
        
        # Szablon produktu do kopiowania
        print(f"\n📝 Szablon produktu:")
        template = client.create_product_template()
        print(json.dumps(template, indent=2, ensure_ascii=False))
        
    except Exception as e:
        print(f"💥 Krytyczny błąd: {e}")
        client.logger.error(f"Nieprzewidziany błąd: {e}", exc_info=True)
