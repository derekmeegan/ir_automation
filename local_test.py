import os
import sys
import json
from typing import Any, Dict
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "serverless", "worker"))
from serverless.worker.handler import lambda_handler

class DummyContext:
    def __init__(self) -> None:
        self.function_name = "local-test"
        self.memory_limit_in_mb = 128
        self.invoked_function_arn = "arn:aws:lambda:local:0:function:local-test"
        self.aws_request_id = "dummy-request-id"

if __name__ == "__main__":
    # Set up environment variables for local testing.
    os.environ["QUARTER"] = '4'
    os.environ["YEAR"] = '2024'
    os.environ["TICKER"] = "ANET"
    os.environ["CSV_URI"] = "s3://bucket/path/to/csv"
    os.environ["DEPLOYMENT_TYPE"] = 'local'
    os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY")
    # SITE_CONFIG should be a JSON string.
    # os.environ["SITE_CONFIG"] = json.dumps({
    #    "base_url": "https://ir.enovix.com/",
    #     "link_template": "https://ir.enovix.com/news-releases/news-release-details/enovix-announces-{quarter}-quarter-{year}-financial-results",
    #     "url_keywords": {
    #         "requires_year": True,
    #         "requires_quarter": True,
    #         "quarter_as_string": True
    #     },
    #     "selectors": ["a"],
    #     "key_phrase": "Envoix Announces",
    #     "refine_link_list": True,
    #     "verify_keywords": {
    #         "requires_year": True,
    #         "requires_quarter": True,
    #         "quarter_as_string": True,
    #         "fixed_terms": ['-quarter']
    #     },
    #     "extraction_method": None,
    #     # "page_content_selector": "div.dialog-off-canvas-main-canvas",
    #     "custom_pdf_edit": None,  # or a function that modifies the PDF text
    #     "llm_instructions": {
    #         "system": (
    #             "You are an assistant that extracts key financial metrics from text, including forward guidance. "
    #             "Return only valid JSON with fields for eps, net_sales, operating_income, net_income, forward_guidance."
    #         ),
    #         "temperature": 0
    #     },
    #     "polling_config": {"interval": 2, "max_attempts": 30}
    # })
    # os.environ["SITE_CONFIG"] = json.dumps({
    #     "base_url": "https://rivian.com/investors",
    #     "link_template": None,
    #     "selectors": ["a.rivian-css-1lzttsb"],
    #     "key_phrase": "Press Release",
    #     "refine_link_list": True,
    #     "verify_keywords": {
    #         "requires_year": True,
    #         "requires_quarter": True,
    #         "quarter_as_string": True,
    #         "fixed_terms": ['-quarter']
    #     },
    #     "extraction_method": None,
    #     "page_content_selector": "main#main",
    #     "custom_pdf_edit": None,  # or a function that modifies the PDF text
    #     "llm_instructions": {
    #         "system": (
    #             "You are an assistant that extracts key financial metrics from text, including forward guidance. "
    #             "Return only valid JSON with fields for eps, net_sales, operating_income, net_income, forward_guidance."
    #         ),
    #         "temperature": 0
    #     },
    #     "polling_config": {"interval": 2, "max_attempts": 30}
    # })
    os.environ["SITE_CONFIG"] = json.dumps({
        "ticker":"ANET",
        "base_url": "https://investors.arista.com/Communications/Press-Releases-and-Events/default.aspx",
        "link_template": "https://s21.q4cdn.com/861911615/files/doc_news/Arista-Networks-Inc.-Reports-{quarter}-Quarter-{year}-Financial-Results-{current_year}.pdf",
        "url_keywords": {
            "requires_year": True,
            "requires_current_year": True, 
            "requires_quarter": True,
            "quarter_as_string": True,
            "quarter_is_title_case": True,
            "fixed_terms": ['-quarter']
        },
        "selectors": ["a.evergreen-news-attachment-PDF"],
        "key_phrase": "Download",
        "refine_link_list": True,
        "verify_keywords": {
            "requires_year": True,
            "requires_quarter": True,
            "quarter_as_string": True,
            "fixed_terms": ['-quarter']
        },
        "extraction_method": 'pdf',
        "custom_pdf_edit": None,  # or a function that modifies the PDF text
        "llm_instructions": {
            "system": """
            You will receive a body of text containing a company's financial report and historical financial metrics. Your task is to:

            1. **Extract Key Financial Metrics:**
            - Revenue (most recent quarter)
            - GAAP EPS (most recent quarter)
            - Non-GAAP EPS (most recent quarter)
            - Forward guidance for revenue and margins (if available)

            2. **Compare Metrics:**
            Provide these metrics in the following format:
            - "Revenue: $X billion"
            - "GAAP EPS: $X"
            - "Non-GAAP EPS: $X"
            - "Forward Guidance: Revenue: $X - $Y billion; Non-GAAP Gross Margin: X% - Y%"

            3. **Classify Sentiment:**
            - Identify forward guidance statements that could impact future performance.
            - Classify them as:
                - "Bullish" if they indicate growth, expansion, or optimistic outlook.
                - "Bearish" if they indicate contraction, risks, or cautious guidance.
                - "Neutral" if guidance is stable or lacks clear directional information.

            4. **Output Structure:**
            Produce the output as a JSON object with the following structure. If there are not ranges for forward guidance, provide the same number twice:
            {
                "metrics": {
                "revenue_billion": X.XX,
                "gaap_eps": X.XX,
                "non_gaap_eps": X.XX,
                "forward_guidance": {
                    "revenue_billion_range": [X.XX, Y.YY],
                    "non_gaap_gross_margin_range": [X, Y],
                    "non_gaap_operating_margin_range": [X, Y]
                }
                },
                "sentiment_snippets": [
                {"snippet": "Text excerpt here", "classification": "Bullish/Bearish/Neutral"}
                ]
            }

            5. **Highlight Context:**
            Include the exact text excerpts as "snippets" from the report that support each sentiment classification.

            Pass this JSON output to the Python function for comparison.
            """,
            "temperature": 0
        },
        "polling_config": {"interval": 2, "max_attempts": 30}
    })

    # Create a dummy event and context.
    event: Dict[str, Any] = {}  # Adjust if you need to pass anything
    context = DummyContext()

    result = lambda_handler(event, context)
    print("Result:", result)
