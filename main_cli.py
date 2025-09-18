import argparse
from playwright.sync_api import sync_playwright
import pandas as pd
from urllib.parse import urljoin, urlparse
import os
import platform
import time
from tqdm import tqdm
import json

class CarrefourScraperCLI:
    def __init__(self):
        self.config = {
            'timeout': 30000,
            'headless': True,
            'user_agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            'max_load_attempts': 20
        }
        self.products = []

    def run(self, url, output_format='csv', output_file=None):
        """Main execution method"""
        print(f"\n🚀 Starting Carrefour Scraper for URL: {url}")
        
        start_time = time.time()
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.config['headless'])
            context = browser.new_context(user_agent=self.config['user_agent'])
            page = context.new_page()
            
            print("\n🌐 Navigating to page...")
            page.goto(url, wait_until="networkidle", timeout=self.config['timeout'])
            
            self.handle_cookies(page)
            
            print("\n🔍 Extracting products...")
            self.extract_all_products(page)
            
            browser.close()
        
        duration = time.time() - start_time
        print(f"\n✅ Completed! Found {len(self.products)} products in {duration:.2f} seconds")
        
        if self.products:
            self.save_results(output_format, output_file)
        else:
            print("⚠️ No products found - check the URL or try again")

    def handle_cookies(self, page):
        """Handle cookie consent banner"""
        try:
            page.click('#onetrust-reject-all-handler', timeout=3000)
            print("✓ Handled cookie consent")
            page.wait_for_timeout(1000)
        except:
            print("⚠️ Could not find cookie consent banner")
            pass

    def extract_all_products(self, page):
        """Extract all products across all pages/load-more/infinite-scroll.

        Strategy:
        1) Extraire les produits visibles.
        2) Tenter successivement:
           - bouton "Afficher les produits suivants" (load more)
           - pagination "Page suivante" (rel=next / aria-label)
           - défilement infini (scroll jusqu'à stabilisation)
        3) Arrêter quand le nombre d'items n'augmente plus ou quand le max d'essais est atteint.
        """
        attempts = 0
        max_attempts = self.config['max_load_attempts']

        def products_count():
            try:
                return page.locator('li.product-list-grid__item').count()
            except Exception:
                return 0

        def wait_for_increase(prev_count: int, timeout_ms: int = 15000, poll_ms: int = 300) -> bool:
            start = time.time()
            while (time.time() - start) * 1000 < timeout_ms:
                new_count = products_count()
                if new_count > prev_count:
                    return True
                page.wait_for_timeout(poll_ms)
            return False

        with tqdm(total=max_attempts, desc="Chargement pages") as pbar:
            # Toujours extraire la première vue
            self.extract_page_products(page)

            while attempts < max_attempts:
                attempts += 1
                pbar.update(1)

                prev = products_count()

                # 1) Bouton "Afficher les produits suivants" (load more)
                try:
                    load_more_selectors = [
                        'button[aria-label="Afficher les produits suivants"]',
                        'button:has-text("Afficher les produits suivants")',
                        'button:has-text("Voir plus")',
                        'button:has-text("Afficher plus")'
                    ]
                    clicked = False
                    for sel in load_more_selectors:
                        btn = page.query_selector(sel)
                        if btn and btn.is_enabled() and btn.is_visible():
                            btn.click()
                            clicked = True
                            break
                    if clicked:
                        if wait_for_increase(prev):
                            self.extract_page_products(page)
                            continue
                except Exception:
                    pass

                # 2) Pagination "Page suivante"
                try:
                    next_selectors = [
                        'a[rel="next"]',
                        'a[aria-label="Page suivante"]',
                        'button[aria-label="Page suivante"]',
                        'a:has-text("Suivant")',
                        'button:has-text("Suivant")'
                    ]
                    clicked = False
                    for sel in next_selectors:
                        el = page.query_selector(sel)
                        if el and el.is_enabled() and el.is_visible():
                            el.click()
                            clicked = True
                            break
                    if clicked:
                        # Attendre navigation/réseau et augmentation des items
                        try:
                            page.wait_for_load_state("networkidle", timeout=self.config['timeout'])
                        except Exception:
                            pass
                        if wait_for_increase(prev):
                            self.extract_page_products(page)
                            continue
                except Exception:
                    pass

                # 3) Défilement infini
                try:
                    # Scroll par paliers jusqu'à ce que plus d'items n'apparaissent
                    increased = False
                    for _ in range(10):
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(700)
                        if products_count() > prev:
                            increased = True
                            prev = products_count()
                        else:
                            break
                    if increased:
                        self.extract_page_products(page)
                        continue
                except Exception:
                    pass

                # Rien de plus à charger
                break

    def extract_page_products(self, page):
        """Extract products from current page"""
        products = page.query_selector_all('li.product-list-grid__item')
        
        for product in products:
            try:
                product_data = {
                    'name': self.safe_extract(product, '.product-list-card-plp-grid__title'),
                    'price': self.extract_price(product),
                    'unit_price': self.safe_extract(product, '.product-list-card-plp-grid__per-unit-label'),
                    'ean': self.extract_ean(product),
                    'nutriscore': self.extract_nutriscore(product),
                    'promo': self.extract_promo(product),
                    'url': self.extract_product_url(product)
                }
                
                if not any(p.get('url') == product_data.get('url') for p in self.products):
                    self.products.append(product_data)
            except:
                continue

    def safe_extract(self, parent, selector, attr=None):
        """Safe data extraction"""
        try:
            element = parent.query_selector(selector)
            if not element:
                return None
                
            if attr:
                return element.get_attribute(attr)
                
            text = element.inner_text().strip()
            return ' '.join(text.split()) if text else None
        except:
            return None

    def extract_price(self, product_element):
        """Extract full price"""
        try:
            whole = self.safe_extract(product_element, '.product-price__content:nth-child(1)')
            decimal = self.safe_extract(product_element, '.product-price__content:nth-child(2)')
            currency = self.safe_extract(product_element, '.product-price__content:nth-child(3)')
            
            if whole and decimal and currency:
                return f"{whole}{decimal} {currency.strip()}"
            
            return self.safe_extract(product_element, '.product-price__amount--main')
        except:
            return None

    def extract_ean(self, product_element):
        """Extract EAN barcode"""
        try:
            article = product_element.query_selector('article')
            if article:
                article_id = article.get_attribute('id')
                if article_id and article_id.isdigit() and len(article_id) == 13:
                    return article_id
            
            url = self.extract_product_url(product_element)
            if url:
                ean = url.split('-')[-1]
                if ean.isdigit() and len(ean) == 13:
                    return ean
            
            return None
        except:
            return None

    def extract_nutriscore(self, product_element):
        """Extract Nutri-Score"""
        try:
            img = product_element.query_selector('.nutriscore-badge img')
            if img:
                src = img.get_attribute('src')
                if 'nutriscore' in src.lower():
                    return src.split('-')[-1][0].upper()
            return None
        except:
            return None

    def extract_promo(self, product_element):
        """Extract promotion info"""
        try:
            promo_selectors = [
                '.sticker-promo__text',
                '.product-card-badge__labels',
                '.promo-badge',
                '[class*="promotion"]',
                '[class*="discount"]'
            ]
            
            for selector in promo_selectors:
                promo_text = self.safe_extract(product_element, selector)
                if promo_text and any(x in promo_text.lower() for x in ['%', '€', 'offre', 'promo']):
                    return promo_text
            
            old_price = self.safe_extract(product_element, '.product-price__amount--old')
            if old_price:
                return f"Ancien prix: {old_price}"
            
            return None
        except:
            return None

    def extract_product_url(self, product_element):
        """Extract complete product URL"""
        try:
            path = product_element.query_selector('a[href^="/p/"]').get_attribute('href')
            if path:
                return urljoin("https://www.carrefour.fr", path)
            return None
        except:
            return None

    def save_results(self, format_type, output_file=None):
        """Save results to file"""
        if not output_file:
            output_file = f"carrefour_products_{int(time.time())}.{format_type}"
        
        df = pd.DataFrame(self.products)
        
        try:
            if format_type == 'csv':
                df.to_csv(output_file, index=False, sep=';', encoding='utf-8-sig')
            elif format_type == 'excel':
                df.to_excel(output_file, index=False)
            elif format_type == 'json':
                df.to_json(output_file, orient='records', indent=2)
            elif format_type == 'txt':
                df.to_csv(output_file, index=False, sep='\t', encoding='utf-8')
            
            print(f"\n💾 Results saved to: {output_file}")
            
            # Print summary to console
            print("\n📊 Summary of extracted data:")
            print(f"- Total products: {len(self.products)}")
            if len(self.products) > 0:
                print(f"- First product: {self.products[0]['name']}")
                print(f"- Average price: {sum(float(p['price'].split()[0].replace(',', '.')) for p in self.products if p['price'])/len(self.products):.2f} €")
            
        except Exception as e:
            print(f"\n❌ Error saving file: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Carrefour Product Scraper CLI")
    def url_type(value: str):
        parsed = urlparse(value)
        if not parsed.scheme or not parsed.netloc:
            raise argparse.ArgumentTypeError("URL invalide. Exemple: https://www.carrefour.fr/s?q=yaourt")
        return value

    parser.add_argument('url', type=url_type, help="URL de la liste produits Carrefour (ex: https://www.carrefour.fr/s?q=yaourt)")
    parser.add_argument('-f', '--format', choices=['csv', 'excel', 'json', 'txt'], 
                       default='csv', help="Output file format")
    parser.add_argument('-o', '--output', help="Output file path")
    parser.add_argument('--no-headless', action='store_true', help="Désactiver le mode headless (utile pour le debug)")
    parser.add_argument('--max-attempts', type=int, default=None, help="Nombre maximum d'essais de chargement/pagination (défaut: config interne)")
    
    args = parser.parse_args()
    
    scraper = CarrefourScraperCLI()
    # Appliquer overrides de configuration depuis la ligne de commande
    if args.no_headless:
        scraper.config['headless'] = False
    if args.max_attempts is not None and args.max_attempts > 0:
        scraper.config['max_load_attempts'] = args.max_attempts

    # Environnements sans serveur X: forcer headless pour éviter l'échec
    if not scraper.config['headless']:
        on_linux = platform.system().lower() == 'linux'
        has_display = bool(os.environ.get('DISPLAY'))
        if on_linux and not has_display:
            print("[INFO] Aucun serveur X détecté (DISPLAY absent) → forçage du mode headless.")
            scraper.config['headless'] = True
    scraper.run(args.url, args.format, args.output)

if __name__ == "__main__":
    main()