import os
import io
import json
import boto3
import asyncio
import PyPDF2
import requests
from groq import Groq
from datetime import datetime
from urllib.parse import urlparse
from typing import Any, Callable, Dict, Optional
from playwright.async_api import async_playwright, TimeoutError

class IRWorkflow:
    def __init__(self, config: Dict[str, Any]):
        """
        config: JSON config with keys such as:
            - base_url: str
            - link_template: Optional[str] (e.g., with placeholders for date, quarter, year)
            - selectors: List[str] for fallback scraping
            - verify_keywords: Dict[str, Any] for quarter/year verification (e.g., {"quarter": "Q3", "year": "24"})
            - extraction_method: 'pdf' or 'html'
            - custom_pdf_edit: Optional[Callable[[str], str]]
            - llm_instructions: Dict[str, Any] (e.g., system prompt, temperature)
            - polling_config: Dict[str, Any] with poll interval settings
        """
        self.base_url: str = config.get("base_url", "")
        self.link_template: Optional[str] = config.get("link_template")
        self.selectors: list[str] = config.get("selectors", ["a.module_link"])
        self.key_phrase: str = config.get("key_phrase", "Shareholder Letter")
        self.verify_keywords = config.get("verify_keywords", {})
        self.url_keywords = config.get("url_keywords", {})
        self.extraction_method: str = config.get("extraction_method", None)
        self.custom_pdf_edit: Optional[Callable[[str], str]] = config.get("custom_pdf_edit")
        self.llm_instructions: Dict[str, Any] = config.get("llm_instructions", {})
        self.polling_config: Dict[str, Any] = config.get("polling_config", {"interval": 60})
        self.refine_link_list = config.get('refine_link_list', False)
        self.page_content_selector = config.get("page_content_selector", "body")
        self.secret_arn = config.get("groq_api_secret_arn")
        self.deployment_type = config.get("deployment_type", "hosted")
        self.groq_api_key = config.get("groq_api_key", self._get_groq_api_key())
        self.discord_webhook_arn = config.get("discord_webhook_arn")
        self.discord_webhook_url = config.get("discord_webhook_url", self._get_discord_webhook_url())
        self.quarter = config.get('quarter')
        self.year = config.get('year')
        self.ticker = config.get('ticker')
        self.json_data = config.get('json_data')
        self.llm_instructions = config.get('llm_instructions')
        self.url_ignore_list = config.get('url_ignore_list', [])

    def _get_discord_webhook_url(self):
        """Retrieve the Discord Webhook URL from AWS Secrets Manager."""
        if self.deployment_type != 'local':
            if self.discord_webhook_arn:
                secrets_client = boto3.client("secretsmanager")
                response = secrets_client.get_secret_value(SecretId=self.discord_webhook_arn)
                secret_dict = json.loads(response["SecretString"])
                return secret_dict.get("DISCORD_WEBHOOK_URL")
            else:
                raise ValueError("Missing DISCORD_WEBHOOK_URL environment variable")

    def _get_groq_api_key(self):
        """Retrieve th e Groq API key from AWS Secrets Manager."""
        if self.deployment_type != 'local':
            if self.secret_arn:
                secrets_client = boto3.client("secretsmanager")
                response = secrets_client.get_secret_value(SecretId=self.secret_arn)
                secret_dict = json.loads(response["SecretString"])
                return secret_dict.get("GROQ_API_KEY")
            else:
                raise ValueError("Missing GROQ_API_SECRET_ARN environment variable")

    def get_base_url(self, url: str) -> str:
        parsed_url = urlparse(url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"
    
    async def _build_link_from_template(self) -> Optional[str]:
        """
        Try to construct the earnings link using a link template and variables.
        """
        if self.link_template:
            print('Link template present, attempting to use it first')
            try:
                keywords = self.url_keywords
                if keywords.get('requires_year', False):
                    keywords.update({'year': self.year})

                if keywords.get('requires_current_year', False):
                    keywords.update({'current_year': datetime.now().strftime('%Y')})

                if keywords.get('requires_quarter', False):
                    quarter = self.quarter
                    if keywords.get('quarter_as_string', False):
                        quarter = {
                            1: 'first',
                            2: 'second',
                            3: 'third',
                            4: 'fourth'
                        }.get(int(float(quarter)))
                        if keywords.get('quarter_is_title_case', False):
                            quarter = quarter.title()
                    elif keywords.get('quarter_with_q', False):
                        quarter = f'Q{quarter}'

                    keywords.update({'quarter': quarter})

                link = self.link_template.format(**self.url_keywords)
                response = requests.head(
                    link, 
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
                )
                if response.ok:
                    return link
            except Exception as e:
                print(f"Error building link from template: {e}")
        return None

    async def _scrape_ir_page_for_link(self) -> Optional[str]:
        """
        Scrapes the IR page up to 5 iterations to find a link matching:
          - a required key phrase (e.g., "Shareholder Letter") in the element text
          - additional indicators (quarter and year) in the link URL (href), with preference:
            both > quarter only > year only.
        On the final iteration, returns the best candidate found (even with lower priority).
        """
        for attempt in range(5):
            print(f"Iteration {attempt+1} of 5")
            candidate_elements = []
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/115.0.0.0 Safari/537.36"
                )
                page = await context.new_page()
                try:
                    await page.goto(self.base_url, wait_until="networkidle", timeout=5000)
                except TimeoutError:
                    print('timeout reached, attempting to pull content thats there')
                    pass
                try:
                    await page.wait_for_selector(self.selectors[0], timeout=5000)
                except Exception as e:
                    print(f"Error waiting for selector '{self.selectors[0]}': {e}")
                for selector in self.selectors:
                    elements = await page.query_selector_all(selector)
                    print(f"Found {len(elements)} elements with selector '{selector}'")
                    for el in elements:
                        text: str = (await el.inner_text()).strip()
                        if self.key_phrase in text:
                            candidate_elements.append((el, text))
                
                # If we want to refine the candidate list (or if no candidates found), check href values
                if self.refine_link_list or not candidate_elements:
                    if candidate_elements:
                        candidates = []
                        search_terms = []
                        keywords = self.verify_keywords
                        if keywords.get('requires_year', False):
                            year = self.year
                            if keywords.get('year_as_two_digits', False):
                                year = str(year)[-2:]

                            search_terms.append(year)

                        if keywords.get('requires_quarter', False):
                            quarter = self.quarter
                            if keywords.get('quarter_as_string', False):
                                quarter = {
                                    1: 'first',
                                    2: 'second',
                                    3: 'third',
                                    4: 'fourth'
                                }.get(int(float(quarter)))
                            elif keywords.get('quarter_with_q', False):
                                quarter = f'Q{quarter}'

                            search_terms.append(quarter)

                        if fixed_terms:= keywords.get('fixed_terms', []):
                            search_terms.extend(fixed_terms)
                            
                        keywords = [str(kw).lower() for kw in search_terms if kw]
                        
                        for el, _ in candidate_elements:
                            href = await el.get_attribute("href")
                            if not href or href in self.url_ignore_list:
                                continue
                            if self.extraction_method == 'pdf' and not href.endswith(self.extraction_method):
                                continue
                            
                            href_lower = href.lower()
                            match_count: int = sum(1 for kw in keywords if kw in href_lower)
                            candidates.append((match_count, el, href))

                        candidates.sort(key=lambda x: x[0], reverse=True)
                        best_priority, best_el, best_href = candidates[0]
                        print(f"Best candidate found with priority {best_priority}: {best_href}")
                        if best_priority > 0 or attempt == 4:
                            print(f"Returning link: {best_href}")
                            await browser.close()
                            return best_href
                        else:
                            print(f"No candidate with sufficient priority found in iteration {attempt+1}")
                    else:
                        print("No candidate elements found in this iteration.")
                else:
                    # If not refining, simply return the href of the first candidate
                    link = await candidate_elements[0][0].get_attribute("href")
                    print("Returning first found link without refining:")
                    await browser.close()
                    return link
                await browser.close()
            await asyncio.sleep(5)
        raise Exception("Earnings link not found after 5 iterations.")

    async def get_earnings_link(self) -> Optional[str]:
        """
        Returns the earnings link either via template or by scraping the page.
        """
        link = await self._build_link_from_template()
        if link:
            return link
        return await self._scrape_ir_page_for_link()

    def extract_pdf_text(self, pdf_url: str) -> str:
        """
        Download PDF and extract text using PyPDF2, applying custom editing if provided.
        """
        response = requests.get(pdf_url)
        response.raise_for_status()
        pdf_bytes: bytes = response.content
        reader: PyPDF2.PdfReader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        text = ""
        for page in reader.pages:
            page_text = page.extract_text() or ""
            text += page_text + "\n"
        if self.custom_pdf_edit:
            text = self.custom_pdf_edit(text)
        return text

    async def extract_html_text(self, url: str) -> str:
        """
        Use Playwright to extract visible text from a web page.
        """
        if url.startswith('/'):
            url = self.get_base_url(self.base_url) + url
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/115.0.0.0 Safari/537.36",
                ignore_https_errors=True
            )
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="networkidle", timeout=10000)
            except TimeoutError:
                print('timeout reached, attempting to pull content thats there')
                pass
            content = await page.inner_text(self.page_content_selector, timeout=10000)
            await browser.close()
            return content

    async def poll_for_earnings_link(self) -> str:
        """
        Continually polls the IR page for the earnings link.
        Default polling interval is 5 seconds.
        """
        interval: int = int(float(self.polling_config.get("interval", 5)))
        max_attempts: int = int(float(self.polling_config.get("max_attempts", 60)))
        for attempt in range(max_attempts):
            try:
                link = await self.get_earnings_link()
                if link:
                    return link
            except Exception as e:
                print(f"Polling attempt {attempt+1}/{max_attempts}: {e}")
                raise
            print(f"Attempt {attempt+1}/{max_attempts}: Link not found, waiting {interval} seconds")
            await asyncio.sleep(interval)
        raise Exception("Link not found after polling")

    def punt_message_to_discord(self, discord_message: str) -> None:
        requests.post(
            self.discord_webhook_url, 
            json={
                "content": discord_message,
                "username": "EarningsEar"
            }
        )
        print('message sent to discord')

    async def process_earnings(self) -> Dict[str, Any]:
        """
        Main workflow: poll for link, extract content (PDF or HTML), and send to LLM for processing.
        """
        link = await self.poll_for_earnings_link()
        if link == "Link not found after polling":
            return {"error": "Earnings link not found"}
        if self.extraction_method == "pdf":
            content = self.extract_pdf_text(link)
        else:
            print('Extracting content from webpage')
            content = await self.extract_html_text(link)
        # Call LLM function asynchronously. Here we assume a separate async function for groq.
        metrics = await self.extract_financial_metrics(content)
        discord_message = self.analyze_financial_metrics(metrics)
        print(discord_message)
        print('punting discord message')
        self.punt_message_to_discord(discord_message)

    def analyze_financial_metrics(self, extracted_data: dict) -> str:
        hist = json.loads(self.json_data)
        print(hist)
        reported = extracted_data['metrics']

        # Determine emojis for comparison
        def compare(actual, estimate):
            if actual > estimate:
                return "🟢"
            elif actual < estimate:
                return "🔴"
            else:
                return "🟡"

        classification_map = {
            'bullish':"🟢",
            'bearish':"🔴",
            'neutral':"🟡"
        }

        # Generate comparisons
        revenue_msg = f"Revenue: ${reported['revenue_billion']}B vs ${hist['current_quarter_sales_estimate_millions']/1000:.2f}B {compare(reported['revenue_billion'], hist['current_quarter_sales_estimate_millions']/1000)}"
        gaap_eps_msg = f"GAAP EPS: ${reported['gaap_eps']} vs ${hist['current_quarter_eps_mean']:.2f} {compare(reported['gaap_eps'], hist['current_quarter_eps_mean'])}"
        non_gaap_eps_msg = f"Non-GAAP EPS: ${reported['non_gaap_eps']} vs ${hist['current_quarter_eps_mean']:.2f} {compare(reported['non_gaap_eps'], hist['current_quarter_eps_mean'])}"

        # Analyze forward guidance
        guidance = reported['forward_guidance']
        forward_revenue_msg = f"Forward Revenue: ${guidance['revenue_billion_range'][0]}B - ${guidance['revenue_billion_range'][1]}B vs ${hist['next_quarter_sales_estimate_millions']/1000:.2f}B"
        forward_gross_margin_msg = f"Forward Non-GAAP Gross Margin: {guidance['non_gaap_gross_margin_range'][0]}% - {guidance['non_gaap_gross_margin_range'][1]}%"
        forward_operating_margin_msg = f"Forward Non-GAAP Gross Margin: {guidance['non_gaap_operating_margin_range'][0]}% - {guidance['non_gaap_operating_margin_range'][1]}%"

        # Analyze sentiment snippets
        sentiment_msgs = "\n".join([f"- {s['snippet']} {classification_map.get(s['classification'].lower(), '🟡')}" for s in extracted_data['sentiment_snippets']])

        # Compose final message
        final_message = (
            f"### ${self.ticker.upper()} Q{self.quarter} Earnings Analysis\n"
            f"{revenue_msg}\n"
            f"{gaap_eps_msg}\n"
            f"{non_gaap_eps_msg}\n\n"
            f"### Forward Guidance\n"
            f"{forward_revenue_msg}\n"
            f"{forward_gross_margin_msg}\n"
            f"{forward_operating_margin_msg}\n\n"
            f"### Sentiment Insights\n"
            f"{sentiment_msgs}"
        )

        return final_message

    async def extract_financial_metrics(self, pdf_text: str) -> Dict[str, Any]:
        """
        Sends the PDF text to a GPT-like service and returns a dictionary
        with extracted financial metrics such as EPS, net sales, and operating income.
        """
        client = Groq(api_key=self.groq_api_key)
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {
                    "role": "system",
                    "content": self.llm_instructions.get('system')
                },
                {
                    "role": "user",
                    "content": pdf_text
                }
            ],
            temperature=int(float(self.llm_instructions.get('temperature'))),
            response_format={"type": "json_object"}
        )
        
        # Extract the model’s message content as text
        content = response.choices[0].message.content
        
        # Attempt to parse JSON from the content
        try:
            metrics = json.loads(content)
        except json.JSONDecodeError:
            metrics = {"error": "Failed to parse metrics from GPT response"}
        
        return metrics