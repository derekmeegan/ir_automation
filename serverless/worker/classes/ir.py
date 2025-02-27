import io
import uuid
import json
import boto3
import base64
import asyncio
import PyPDF2
import requests
from groq import Groq, BadRequestError
from datetime import datetime
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional
from playwright.async_api import async_playwright

class IRWorkflow:
    def __init__(self, config: Dict[str, Any]):
        """
        config: JSON config with keys such as:
            - base_url: str
            - link_template: Optional[str] (e.g., with placeholders for date, quarter, year)
            - selectors: List[str] for fallback scraping
            - verify_keywords: Dict[str, Any] for quarter/year verification (e.g., {"quarter": "Q3", "year": "24"})
            - extraction_method: 'pdf' or 'html'
            - custom_pdf_edit: Optional[[[str], str]]
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
        self.custom_pdf_edit = config.get("custom_pdf_edit")
        self.llm_instructions: Dict[str, Any] = config.get("llm_instructions", {})
        self.polling_config: Dict[str, Any] = config.get("polling_config", {"interval": 60})
        self.refine_link_list = config.get('refine_link_list', False)
        self.page_content_selector = config.get("page_content_selector", "body")
        self.secret_arn = config.get("groq_api_secret_arn")
        self.deployment_type = config.get("deployment_type", "hosted")
        self.groq_api_key = config.get("groq_api_key") or self._get_groq_api_key()
        self.discord_webhook_arn = config.get("discord_webhook_arn")
        self.discord_webhook_url = config.get("discord_webhook_url") or self._get_discord_webhook_url()
        self.quarter = config.get('quarter')
        self.year = config.get('year')
        self.ticker = config.get('ticker')
        self.json_data = config.get('json_data')
        self.llm_instructions = config.get('llm_instructions')
        self.url_ignore_list = config.get('url_ignore_list', [])
        self.href_ignore_words = config.get('href_ignore_words', [])
        self.original_config: Dict[str, Any] = config.copy()
        self.s3_artifact_bucket = config.get("s3_artifact_bucket")
        self.browser = (config.get('browser_type') or 'chromium').lower()
        self.past_browser = None
        self.messages_table = config.get('messages_table')

    def _get_discord_webhook_url(self):
        """Retrieve the Discord Webhook URL from AWS Secrets Manager."""
        if self.deployment_type != 'local':
            if self.discord_webhook_arn:
                secrets_client = boto3.client("secretsmanager", region_name='us-east-1')
                response = secrets_client.get_secret_value(SecretId=self.discord_webhook_arn)
                secret_dict = json.loads(response["SecretString"])
                return secret_dict.get("DISCORD_WEBHOOK_URL")
            else:
                raise ValueError("Missing DISCORD_WEBHOOK_URL environment variable")

    def _get_groq_api_key(self):
        """Retrieve th e Groq API key from AWS Secrets Manager."""
        if self.deployment_type != 'local':
            if self.secret_arn:
                secrets_client = boto3.client("secretsmanager", region_name='us-east-1')
                response = secrets_client.get_secret_value(SecretId=self.secret_arn)
                secret_dict = json.loads(response["SecretString"])
                return secret_dict.get("GROQ_API_KEY")
            else:
                raise ValueError("Missing GROQ_API_SECRET_ARN environment variable")

    def store_message_to_dynamo(self, message: str) -> None:
        """
        Write the discord message to a DynamoDB table specified by self.messages_table.
        """
        timestamp: str = datetime.now().isoformat()
        message_id: str = str(uuid.uuid4())

        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.Table(self.messages_table)
        
        try:
            table.put_item(
                Item={
                    "message_id": message_id,
                    "ticker": self.ticker,
                    "quarter": self.quarter,
                    "year": self.year,
                    "timestamp": timestamp,
                    "discord_message": message
                }
            )
            print(f"Stored discord message with id {message_id} in table.")
        except Exception as e:
            print(f"Error storing discord message to DynamoDB: {e}")

    def get_base_url(self, url: str) -> str:
        parsed_url = urlparse(url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    def switch_browser(self):
        if self.browser == 'chromium' and not self.past_browser:
            self.browser = 'firefox'
        elif self.browser == 'firefox' and not self.past_browser:
            self.browser = 'chromium'

        print('already switched browser and still doesnt work... ur cooked')
        return

    def get_browser(self, p):
        if self.browser == 'firefox':
            return p.firefox
            
        return p.chromium
        
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
        for attempt in range(8):
            print(f"Iteration {attempt+1} of 8")
            candidate_elements = []
            async with async_playwright() as p:
                print('launching browser')
                browser = self.get_browser(p)
                if attempt > 1:
                    print(f'{self.browser} aint working, switching to a different one')
                    self.switch_browser()
                    browser = self.get_browser(p)

                browser = await browser.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-gpu",
                        "--single-process",
                        "--disable-dev-shm-usage",
                        "--disable-software-rasterizer",
                        "--disable-setuid-sandbox",
                        "--disable-features=SitePerProcess",
                        "--headless=new"
                    ]
                )
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/121.0.0.0 Safari/537.36",
                    ignore_https_errors=True
                )
                page = await context.new_page()
                try:
                    await page.goto(self.base_url, wait_until="networkidle", timeout=5000)
                except Exception as e:
                    print(f"Error during page.goto (networkidle): {e}")
                    try:
                        await page.goto(self.base_url, wait_until="domcontentloaded", timeout=10000)
                    except Exception as inner_e:
                        print(f"Fallback navigation failed: {inner_e}")
                    print('timeout reached, attempting to pull content thats there')
                    pass

                try:
                    await page.wait_for_selector(self.selectors[0], timeout=5000)
                except Exception as e:
                    print(f"Error waiting for selector '{self.selectors[0]}': {e}")

                print('Extracted page content')
                for selector in self.selectors:
                    elements = []
                    try:
                        elements = await page.query_selector_all(selector)
                    except Exception as e:
                        print('error reading selectors from page')
                    print(f"Found {len(elements)} elements with selector '{selector}'")
                    for el in elements:
                        text: str = (await el.inner_text()).strip()
                        if self.key_phrase.lower() in text.lower():
                            candidate_elements.append((el, text))
                
                # If we want to refine the candidate list (or if no candidates found), check href values
                if self.refine_link_list or not candidate_elements:
                    if candidate_elements:
                        print('Refining candidate elements')
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
                            if any(ignore_word.lower() in href.lower() for ignore_word in self.href_ignore_words):
                                continue
                            
                            href_lower = href.lower()
                            match_count: int = sum(1 for kw in keywords if kw in href_lower)
                            candidates.append((match_count, el, href))

                        candidates.sort(key=lambda x: x[0], reverse=True)
                        best_priority, best_el, best_href = candidates[0]
                        print(f"Best candidate found with priority {best_priority}: {best_href}")
                        if best_priority > 0:
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

        print('Template not available, scraping from IR site')
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
        if url.startswith('/'):
            url = self.get_base_url(self.base_url) + url
        for attempt in range(8):
            print(f"Iteration {attempt+1} of 8")
            async with async_playwright() as p:
                browser_type = self.get_browser(p)
                if attempt > 1:
                    print(f"{self.browser} not working... switching to a different one")
                    self.switch_browser()
                    browser_type = self.get_browser(p)

                browser = await browser_type.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-gpu",
                        "--single-process",
                        "--disable-dev-shm-usage",
                        "--disable-software-rasterizer",
                        "--disable-setuid-sandbox",
                        "--disable-features=SitePerProcess",
                        "--headless=new"
                    ]
                )
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/115.0.0.0 Safari/537.36",
                    ignore_https_errors=True
                )
                page = await context.new_page()
                try:
                    await page.goto(url, wait_until="networkidle", timeout=10000)
                except Exception as e:
                    print(f"Error during page.goto (networkidle): {e}")
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=10000)
                    except Exception as inner_e:
                        print(f"Fallback navigation failed: {inner_e}")
                    print("timeout reached, attempting to pull content that's there")
                try:
                    content: str = await page.inner_text(self.page_content_selector, timeout=10000)
                    await browser.close()
                    return content
                except Exception as e:
                    print(f"Error extracting content: {e}")
                    await browser.close()
        return ""


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

        if not content:
            raise Exception('Content was not able to be scraped')
        metrics = await self.extract_financial_metrics(content)
        message = self.analyze_financial_metrics(metrics)
        print('punting discord message')
        self.punt_message_to_discord(message)
        self.store_artifacts(
            scraped_url=link,
            scraped_content=content,
            groq_response=metrics,
            discord_message=message
        )
        self.store_message_to_dynamo(message)

    def analyze_financial_metrics(self, extracted_data: dict) -> str:
        hist: Dict[str, Any] = json.loads(self.json_data)
        metrics: Dict[str, Any] = extracted_data.get("metrics", {})

        def compare(actual: float, estimate: Optional[float]) -> str:
            if estimate is None:
                return "🟡"
            if actual > estimate:
                return "🟢"
            if actual < estimate:
                return "🔴"
            return "🟡"

        messages: List[str] = []

        # Process current quarter metrics
        current: Dict[str, Any] = metrics.get("current_quarter", {})
        for key, actual in current.items():
            hist_val: Optional[float] = hist.get(f"current_{key}")
            comp: str = compare(actual, hist_val)
            messages.append(f"{key.replace('billion', '').replace('_', ' ').title()}: ${actual}{'B' if 'billion' in key else ''} vs {hist_val}{'B' if 'billion' in key else ''} {comp}")

        messages.append('\n')

        # Process full year metrics, if available
        full_year: Dict[str, Any] = metrics.get("full_year", {})
        for key, actual in full_year.items():
            hist_val: Optional[float] = hist.get(f"full_year_{key}")
            comp: str = compare(actual, hist_val)
            messages.append(f"Full Year {key.replace('_billion', '').replace('_', ' ').title()}: ${actual}{'B' if 'billion' in key else ''} vs {hist_val}{'B' if 'billion' in key else ''} {comp}")

        # Process forward guidance metrics
        forward_guidance: Dict[str, Any] = metrics.get("forward_guidance", {})
        forward_messages: List[str] = []
        for period, guidance in forward_guidance.items():
            period_msgs: List[str] = []
            for key, value in guidance.items():
                if isinstance(value, list):
                    if len(value) == 1:
                        value = [value[0], value[0]]
                    period_msgs.append(f"{key.replace('_', ' ').title()}: {value[0]:.2f}B - {value[1]:.2f}B")
                else:
                    period_msgs.append(f"{key.replace('_', ' ').title()}: {value}")
            forward_messages.append(f"\n{period.replace('_', ' ').title()}:\n" + "\n".join(period_msgs))

        sentiment_snippets: List[Dict[str, str]] = extracted_data.get("sentiment_snippets", [])
        classification_map: Dict[str, str] = {'bullish': "🟢", 'bearish': "🔴", 'neutral': "🟡"}
        sentiment_msgs: str = "\n".join(
            [f"- {s.get('snippet', '')} {classification_map.get(s.get('classification', '').lower(), '🟡')}" 
            for s in sentiment_snippets]
        )

        final_message: str = (
            f"### ${self.ticker.upper()} Q{self.quarter} Earnings Analysis\n"
            f"{chr(10).join(messages)}\n\n"
            f"### Forward Guidance\n"
            f"{chr(10).join(forward_messages)}\n\n"
            f"### Sentiment Insights\n"
            f"{sentiment_msgs}"
        )
        return final_message[:2000]

    async def extract_financial_metrics(self, content: str) -> Dict[str, Any]:
        """
        Sends the PDF text to a GPT-like service and returns a dictionary
        with extracted financial metrics such as EPS, net sales, and operating income.
        Retries the API call on JSON decode or BadRequest errors.
        """
        max_attempts: int = 3
        delay: float = 1.0

        prompt = self.llm_instructions.get('system')
        if self.deployment_type != 'local':
            prompt = base64.b64decode(prompt).decode("utf-8")

        for attempt in range(max_attempts):
            try:
                client = Groq(api_key=self.groq_api_key)
                response = client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=[
                        {
                            "role": "system",
                            "content": f'''
                            Follow the below steps unless the provided information is empty or does not contain the earnings press release for {self.ticker} for {self.quarter} {self.year}
                            DO NOT MAKE UP METRICS, ONLY USE NUMBERS PROVIDED IN THE TEXT
                            {prompt}
                            '''
                        },
                        {
                            "role": "user",
                            "content": content
                        }
                    ],
                    temperature=int(float(self.llm_instructions.get('temperature'))),
                    response_format={"type": "json_object"}
                )

                content: str = response.choices[0].message.content
                metrics: Dict[str, Any] = json.loads(content)
                return metrics

            except (json.JSONDecodeError, BadRequestError) as e:
                if attempt == max_attempts - 1:
                    return {"error": f"Failed to parse metrics from GPT response after retries: {e}"}
                await asyncio.sleep(delay)
                delay *= 2

    def store_artifacts(
        self,
        scraped_url: str,
        scraped_content: str,
        groq_response: Dict[str, Any],
        discord_message: str
    ) -> None:
        if self.deployment_type != 'local':
            timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name: str = f"{self.ticker}_{timestamp}.json"
            stored_config: Dict[str, Any] = self.original_config.copy()
            if "llm_instructions" in stored_config and "system" in stored_config["llm_instructions"]:
                try:
                    stored_config["llm_instructions"]["system"] = base64.b64decode(
                        stored_config["llm_instructions"]["system"]
                    ).decode("utf-8")
                except Exception:
                    pass

            artifact: Dict[str, Any] = {
                "ticker": self.ticker,
                "timestamp": timestamp,
                "scraped_url": scraped_url,
                "scraped_content": scraped_content,
                "groq_response": groq_response,
                "discord_message": discord_message,
                "config": stored_config
            }
            artifact_json: str = json.dumps(artifact)
            s3_bucket = self.s3_artifact_bucket
            s3_client = boto3.client("s3")
            s3_client.put_object(Bucket=s3_bucket, Key=file_name, Body=artifact_json)
            print(f"Artifacts stored in S3 bucket '{s3_bucket}' with key '{file_name}'")