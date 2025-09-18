import argparse
from playwright.sync_api import sync_playwright
import pandas as pd
from urllib.parse import urljoin, urlparse, urlencode, parse_qsl, urlunparse
import os
import platform
import re
import time
from tqdm import tqdm
import json
from concurrent.futures import ProcessPoolExecutor, as_completed

class CarrefourScraperCLI:
    def __init__(self):
        self.config = {
            'timeout': 30000,
            'headless': True,
            'user_agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            'max_load_attempts': 20
        }
        self.products = []
        self.seen_urls = set()

    def run(self, url, output_format='csv', output_file=None, single_page_only: bool = False):
        print(f"\n🚀 Starting Carrefour Scraper for URL: {url}")
        start_time = time.time()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.config['headless'])
            context = browser.new_context(user_agent=self.config['user_agent'])
            page = context.new_page()

            print("\n🌐 Navigating to page...")
            page.goto(url, wait_until="networkidle", timeout=self.config['timeout'])

            self.handle_cookies(page)
            try:
                page.wait_for_selector('li.product-list-grid__item', timeout=10000)
            except Exception:
                pass

            print("\n🔍 Extracting products...")
            if single_page_only:
                self.extract_page_products(page, start_index=0)
            else:
                self.extract_all_products(page)

            browser.close()

        duration = time.time() - start_time
        print(f"\n✅ Completed! Found {len(self.products)} products in {duration:.2f} seconds")

        if self.products:
            self.save_results(output_format, output_file)
        else:
            print("⚠️ No products found - check the URL or try again")

    def handle_cookies(self, page):
        try:
            page.click('#onetrust-reject-all-handler', timeout=3000)
            print("✓ Handled cookie consent")
            page.wait_for_timeout(1000)
        except:
            print("⚠️ Could not find cookie consent banner")
            pass

    def extract_all_products(self, page):
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
            self.extract_page_products(page, start_index=0)

            while attempts < max_attempts:
                attempts += 1
                pbar.update(1)

                prev = products_count()

                # Load more
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
                            self.extract_page_products(page, start_index=prev)
                            continue
                except Exception:
                    pass

                # Next page
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
                        try:
                            page.wait_for_load_state("networkidle", timeout=self.config['timeout'])
                        except Exception:
                            pass
                        if wait_for_increase(prev):
                            self.extract_page_products(page, start_index=prev)
                            continue
                except Exception:
                    pass

                # Infinite scroll
                try:
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
                        self.extract_page_products(page, start_index=prev)
                        continue
                except Exception:
                    pass

                break

    def extract_page_products(self, page, start_index: int = 0):
        products = page.query_selector_all('li.product-list-grid__item')
        if start_index > 0:
            products = products[start_index:]

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
                url = product_data.get('url')
                if url and url not in self.seen_urls:
                    self.products.append(product_data)
                    self.seen_urls.add(url)
            except:
                continue

    def safe_extract(self, parent, selector, attr=None):
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
        try:
            path = product_element.query_selector('a[href^="/p/"]').get_attribute('href')
            if path:
                return urljoin("https://www.carrefour.fr", path)
            return None
        except:
            return None

    def save_results(self, format_type, output_file=None):
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
            print("\n📊 Summary of extracted data:")
            print(f"- Total products: {len(self.products)}")
            if len(self.products) > 0:
                print(f"- First product: {self.products[0]['name']}")
                try:
                    avg_price = sum(float(p['price'].split()[0].replace(',', '.')) for p in self.products if p.get('price'))/max(1, len(self.products))
                    print(f"- Average price: {avg_price:.2f} €")
                except Exception:
                    pass
        except Exception as e:
            print(f"\n❌ Error saving file: {str(e)}")


def _slugify_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.replace('/', '_').strip('_')
    query = re.sub(r'[^a-zA-Z0-9]+', '_', parsed.query).strip('_')
    base = f"{parsed.netloc}_{path}" if path else parsed.netloc
    if query:
        base = f"{base}_{query[:80]}"
    return base.lower().strip('_')


def _ensure_output_path(output_dir: str | None, file_name: str) -> str:
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        return os.path.join(output_dir, file_name)
    return file_name


def process_url_task(task):
    url, fmt, output_dir, no_headless, max_attempts, single_page_only, output_file, return_products = task
    scraper = CarrefourScraperCLI()
    if no_headless:
        scraper.config['headless'] = False
    if max_attempts is not None and max_attempts > 0:
        scraper.config['max_load_attempts'] = max_attempts
    if not scraper.config['headless']:
        on_linux = platform.system().lower() == 'linux'
        has_display = bool(os.environ.get('DISPLAY'))
        if on_linux and not has_display:
            print("[INFO] Aucun serveur X détecté (DISPLAY absent) → forçage du mode headless.")
            scraper.config['headless'] = True
    if output_file:
        out_file = output_file
    else:
        stamp = int(time.time())
        base = _slugify_url(url)
        out_file = _ensure_output_path(output_dir, f"{base}_{stamp}.{fmt}")
    scraper.run(url, fmt, out_file, single_page_only=single_page_only)
    result = {
        'url': url,
        'output': out_file,
        'count': len(scraper.products)
    }
    if return_products:
        result['products'] = scraper.products
    return result


def build_page_url(base_url: str, page_num: int) -> str:
    parsed = urlparse(base_url)
    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    q['page'] = str(page_num)
    new_query = urlencode(q, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def main():
    parser = argparse.ArgumentParser(description="Carrefour Product Scraper CLI (Optimized)")
    def url_type(value: str):
        parsed = urlparse(value)
        if not parsed.scheme or not parsed.netloc:
            raise argparse.ArgumentTypeError("URL invalide. Exemple: https://www.carrefour.fr/s?q=yaourt")
        return value

    parser.add_argument('urls', nargs='+', type=url_type, help="Une ou plusieurs URLs de liste produits Carrefour")
    parser.add_argument('-f', '--format', choices=['csv', 'excel', 'json', 'txt'], 
                       default='csv', help="Format du fichier de sortie")
    parser.add_argument('-o', '--output', help="Chemin du fichier de sortie (mode 1 URL). Pour multi-URL, utilisez --output-dir")
    parser.add_argument('--output-dir', help="Dossier de sortie (mode multi-URL) — un fichier par URL y sera créé")
    parser.add_argument('--no-headless', action='store_true', help="Désactiver le mode headless (utile pour le debug)")
    parser.add_argument('--max-attempts', type=int, default=None, help="Nombre maximum d'essais de chargement/pagination (défaut: config interne)")
    parser.add_argument('--workers', type=int, default=3, help="Nombre de tâches parallèles (multi-URL ou mono-URL sharding)")
    parser.add_argument('--parallel-single', action='store_true', help="Activer le parallélisme sur un seul lien en shardant par pages")
    parser.add_argument('--max-pages', type=int, default=12, help="Nombre maximum de pages à sharder pour un seul lien (si --parallel-single)")

    args = parser.parse_args()

    if len(args.urls) == 1:
        url = args.urls[0]
        if not args.parallel_single:
            scraper = CarrefourScraperCLI()
            if args.no_headless:
                scraper.config['headless'] = False
            if args.max_attempts is not None and args.max_attempts > 0:
                scraper.config['max_load_attempts'] = args.max_attempts
            if not scraper.config['headless']:
                on_linux = platform.system().lower() == 'linux'
                has_display = bool(os.environ.get('DISPLAY'))
                if on_linux and not has_display:
                    print("[INFO] Aucun serveur X détecté (DISPLAY absent) → forçage du mode headless.")
                    scraper.config['headless'] = True
            scraper.run(url, args.format, args.output)
            return
        else:
            print(f"[INFO] Parallélisation mono-URL activée → {args.workers} workers, {args.max_pages} pages max")
            shard_urls = [build_page_url(url, p) for p in range(1, max(2, args.max_pages) + 1)]
            tasks = []
            for shard in shard_urls:
                tasks.append((shard, args.format, args.output_dir, args.no_headless, args.max_attempts, True, None, True))
            results = []
            with ProcessPoolExecutor(max_workers=max(1, args.workers)) as executor:
                future_map = {executor.submit(process_url_task, t): t[0] for t in tasks}
                for future in as_completed(future_map):
                    su = future_map[future]
                    try:
                        res = future.result()
                        results.append(res)
                        print(f"[OK] shard {su} → {res['count']} produits")
                    except Exception as e:
                        print(f"[ERREUR] shard {su} → {e}")
            all_products = []
            seen = set()
            for r in results:
                for p in r.get('products', []) if r.get('products') else []:
                    u = p.get('url')
                    if u and u not in seen:
                        seen.add(u)
                        all_products.append(p)
            print(f"\n✅ Fusion des shards: {len(all_products)} produits uniques")
            if args.output:
                final_out = args.output
            else:
                base = _slugify_url(url)
                final_out = _ensure_output_path(args.output_dir, f"{base}_merged_{int(time.time())}.{args.format}")
            df = pd.DataFrame(all_products)
            if args.format == 'csv':
                df.to_csv(final_out, index=False, sep=';', encoding='utf-8-sig')
            elif args.format == 'excel':
                df.to_excel(final_out, index=False)
            elif args.format == 'json':
                df.to_json(final_out, orient='records', indent=2)
            elif args.format == 'txt':
                df.to_csv(final_out, index=False, sep='\t', encoding='utf-8')
            print(f"💾 Fichier final: {final_out}")
            return

    # Multi-URL: exécution parallèle
    tasks = []
    for url in args.urls:
        tasks.append((url, args.format, args.output_dir, args.no_headless, args.max_attempts, False, None, False))
    results = []
    with ProcessPoolExecutor(max_workers=max(1, args.workers)) as executor:
        future_map = {executor.submit(process_url_task, t): t[0] for t in tasks}
        for future in as_completed(future_map):
            url = future_map[future]
            try:
                res = future.result()
                results.append(res)
                print(f"\n[OK] {url} → {res['count']} produits, fichier: {res['output']}")
            except Exception as e:
                print(f"\n[ERREUR] {url} → {e}")
    total = sum(r.get('count', 0) for r in results)
    print(f"\n✅ Terminé: {len(results)} URLs traitées, {total} produits au total.")


if __name__ == "__main__":
    main()
