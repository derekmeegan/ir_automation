import io
import uuid
import json
import boto3
import base64
import asyncio
import PyPDF2
import requests
from groq import Groq, BadRequestError
from datetime import datetime, timezone
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

class IRWorkflow:
    def __init__(self, config: Dict[str, Any]):
        """
        config: JSON config with keys such as:
            - base_url: str
            - selectors: List[str] for fallback scraping
            - verify_keywords: Dict[str, Any] for quarter/year verification (e.g., {"quarter": "Q3", "year": "24"})
            - extraction_method: 'pdf' or 'html'
            - custom_pdf_edit: Optional[[[str], str]]
            - llm_instructions: Dict[str, Any] (e.g., system prompt, temperature)
        """
        self.base_url: str = config.get("base_url", "")
        self.selector: list[str] = config.get('selector', 'a')
        self.verify_keywords = config.get("verify_keywords", {})
        self.extraction_method: str = config.get("extraction_method", None)
        self.llm_instructions: Dict[str, Any] = config.get("llm_instructions", {})
        self.page_content_selector = config.get("page_content_selector", "body")
        self.groq_api_secret_arn = config.get("groq_api_secret_arn")
        self.deployment_type = config.get("deployment_type", "hosted")
        self.groq_api_key = config.get("groq_api_key") or self._get_groq_api_key()
        self.discord_webhook_arn = config.get("discord_webhook_arn")
        self.discord_webhook_url = config.get("discord_webhook_url") or self._get_discord_webhook_url()
        self.quarter = config.get('quarter')
        self.year = config.get('year')
        self.ticker = config.get('ticker')
        self.json_data = config.get('json_data')
        self.url_ignore_list = config.get('url_ignore_list', [])
        self.href_ignore_words = config.get('href_ignore_words', [])
        self.original_config: Dict[str, Any] = config.copy()
        self.s3_artifact_bucket = config.get("s3_artifact_bucket")
        self.browser = config.get('browser_type', 'chromium').lower()
        self.messages_table = config.get('messages_table')
        self.message = None

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
            if self.groq_api_secret_arn:
                secrets_client = boto3.client("secretsmanager", region_name='us-east-1')
                response = secrets_client.get_secret_value(SecretId=self.groq_api_secret_arn)
                secret_dict = json.loads(response["SecretString"])
                return secret_dict.get("GROQ_API_KEY")
            else:
                raise ValueError("Missing GROQ_API_SECRET_ARN environment variable")

    def store_message_to_dynamo(self, message: str) -> None:
        """
        Write the discord message to a DynamoDB table specified by self.messages_table.
        """
        if self.deployment_type != 'local':
            timestamp: str = datetime.now(timezone.utc).isoformat()
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

    async def launch_browser_and_open_page(self, p, browser = None):
        if browser is None:
            print(f'Launching browser: {self.browser}')
            browser = p.chromium    
            if self.browser == 'firefox':
                browser = p.firefox

            browser = await browser.launch(
                headless=True,
                args=[
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--disable-software-rasterizer",
                    "--headless=new"
                ]
            )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/121.0.0.0 Safari/537.36",
            ignore_https_errors=True,
            locale='en-US',
            bypass_csp=True,
            java_script_enabled=True
        )
        
        page = await context.new_page()
        await page.route("**/*", lambda route: route.abort()
            if route.request.resource_type in ["image", "stylesheet", "font"]
            else route.continue_())
            
        return page, browser

    async def _scrape_ir_page_for_link(self, page) -> Optional[str]:
        attempt = 0
        domcontentloaded_timeout_count = 0
        waitforselector_timeout_count = 0
        # async with async_playwright() as p:
        while True:
            if attempt == 8:
                break
            print(f"Iteration {attempt+1} of 8")

            try:
                timeout = 5_000 if domcontentloaded_timeout_count < 3 else 10_000
                await page.goto(self.base_url, wait_until="domcontentloaded", timeout=timeout)
            except PlaywrightTimeoutError as e:
                print('timeout reached, attempting to pull content thats there')
                domcontentloaded_timeout_count += 1
            except Exception as e:
                print(f"Error during page.goto (networkidle): {e}")

            try:
                timeout = 5_000 if waitforselector_timeout_count < 3 else 10_000
                await page.wait_for_selector(self.selector, timeout=timeout)
            except PlaywrightTimeoutError as e:
                print(f"Timeout waiting for selector '{self.selector}': {e}")
                waitforselector_timeout_count += 1
            except Exception as e:
                print(f"Error waiting for selector '{self.selector}': {e}")
                attempt+=1

            print('Extracted page content')
            elements = []
            try:
                elements = await page.query_selector_all(self.selector)
                print(f"Found {len(elements)} elements with selector '{self.selector}'")
            except Exception as e:
                print('error reading selectors from page')

            if not elements:
                print(f"No elements found in in iteration {attempt+1}. Decrementing 1 from the attempt.")
                attempt -=1
                continue
            
            print('Refining element list')
            keywords = self._generate_search_keywords()
            candidates = []
            for el in elements:
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
                return best_href
            else:
                print(f"No candidate with sufficient priority found in iteration {attempt+1}. Decrementing 1 from the attempt")
                attempt -=1
        
        attempt +=1
        await asyncio.sleep(3)
        raise Exception(f"Earnings link not found after {attempt+1} iterations.")

    def _generate_search_keywords(self) -> List[str]:
        """
        Generate a list of search keywords based on verification criteria.
        
        Returns:
            List of lowercase keywords to search for in URLs
        """
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

        if fixed_terms := keywords.get('fixed_terms', []):
            search_terms.extend(fixed_terms)
            
        return [str(kw).lower() for kw in search_terms if kw]

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
        return text

    async def extract_html_text(self, url: str, page) -> str:
        if url.startswith('/'):
            url = self.get_base_url(self.base_url) + url

        # async with async_playwright() as p:
        domcontentloaded_timeout_count = 0
        pagerinnertext_timeout_count = 0
        for attempt in range(8):
            print(f"Iteration {attempt+1} of 8")
            try:
                timeout = 5_000 if domcontentloaded_timeout_count < 3 else 10_000
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            except Exception as e:
                print(
                    f"Error during page.goto (domcontentloaded): {e}"
                    "timeout reached, attempting to pull content that's there"
                )
                domcontentloaded_timeout_count += 1
            try:
                timeout = 10_000 if pagerinnertext_timeout_count < 3 else 20_000
                content: str = await page.inner_text(self.page_content_selector, timeout=timeout)
                if content or attempt > 6:
                    return content
            except PlaywrightTimeoutError as e:
                print(f"Timeout waiting for content")
                pagerinnertext_timeout_count += 1
            except Exception as e:
                print(f"Error extracting content: {e}")
        return ""

    def punt_message_to_discord(self, discord_message: str) -> None:
        if self.deployment_type != 'local':
            requests.post(
                self.discord_webhook_url, 
                json={
                    "content": discord_message,
                    "username": "EarningsEar"
                }
            )
            print('message sent to discord')

    async def extract_earnings_content(self, link: str, p, browser) -> str:
        content = None
        if self.extraction_method == "pdf":
            content = self.extract_pdf_text(link)
        else:
            print('Extracting content from webpage')
            page, browser = await self.launch_browser_and_open_page(p, browser = browser)
            content = await self.extract_html_text(link, page)
            await browser.close()

        if not content:
            raise Exception('Content was not able to be scraped')

        return content

    def analyze_financial_metrics(self, extracted_data: dict) -> str:
        hist: Dict[str, Any] = json.loads(self.json_data)
        metrics: Dict[str, Any] = extracted_data.get("metrics", {})
        if not metrics.get("current_quarter") and not metrics.get("full_year") and not metrics.get("forward_guidance").get('next_quarter') and not metrics.get("forward_guidance").get('fiscal_year'):
            raise Exception('Looks like earnings were attempting to be scraped from the wrong link.')

        def compare(actual: float, estimate: Optional[float]) -> str:
            if actual is None:
                actual = 0

            if estimate is None:
                estimate = 0

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
            messages.append(
                f"{key.replace('billion', '').replace('_', ' ').title()}: ${actual}{'B' if 'billion' in key else ''} vs {hist_val}{'B' if 'billion' in key else ''} {comp}"
            )

        messages.append('\n')

        # Process full year metrics, if available
        full_year: Dict[str, Any] = metrics.get("full_year", {})
        for key, actual in full_year.items():
            hist_val: Optional[float] = hist.get(f"full_year_{key}")
            comp: str = compare(actual, hist_val)
            messages.append(
                f"Full Year {key.replace('_billion', '').replace('_', ' ').title()}: ${actual}{'B' if 'billion' in key else ''} vs {hist_val}{'B' if 'billion' in key else ''} {comp}"
            )

        # Process forward guidance metrics
        forward_guidance: Dict[str, Any] = metrics.get("forward_guidance", {})
        forward_messages: List[str] = []
        for period, guidance in forward_guidance.items():
            period_msgs: List[str] = []
            for key, value in guidance.items():
                if isinstance(value, dict) and value.get('low') and value.get('high'):
                    period_msgs.append(
                        f"{key.replace('_', ' ').title()}: {value['low']:.2f}B - {value['high']:.2f}B"
                    )
                elif isinstance(value, list) and all(value):
                    if len(value) == 1:
                        value = [value[0], value[0]]
                    period_msgs.append(
                        f"{key.replace('_', ' ').title()}: {value[0]:.2f}B - {value[1]:.2f}B"
                    )
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
        self.message = final_message[:2000]
        return self.message

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
                            DO NOT MAKE UP METRICS, ONLY USE NUMBERS PROVIDED IN THE CONTENT OF THE NEXT MESSAGE
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
        timestamp: str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
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
            
    async def process_earnings(self) -> Dict[str, Any]:
        """
        Main workflow: poll for link, extract content (PDF or HTML), and send to LLM for processing.
        """
        async with async_playwright() as p:
            page, browser = await self.launch_browser_and_open_page(p)
            link = await self._scrape_ir_page_for_link(page)
            content = await self.extract_earnings_content(link, p, browser)

        metrics = await self.extract_financial_metrics(content)
        message = self.analyze_financial_metrics(metrics)
        self.punt_message_to_discord(message)
        # self.store_artifacts(
        #     scraped_url=link,
        #     scraped_content=content,
        #     groq_response=metrics,
        #     discord_message=message
        # )
        self.store_message_to_dynamo(message)