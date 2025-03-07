import os
import sys
import pytest

# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "services", "worker"))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services.worker.classes.ir import IRWorkflow

os.environ["SCREEN_WIDTH"] = '1920'
os.environ["SCREEN_HEIGHT"] = '1024'
os.environ["SCREEN_DEPTH"] = '16'
os.environ["MAX_CONCURRENT_CHROME_PROCESSES"]='10'
os.environ["ENABLE_DEBUGGER"] = 'false'
os.environ["PREBOOT_CHROME"] = 'true'
os.environ["CONNECTION_TIMEOUT"] = '300000'
os.environ["MAX_CONCURRENT_SESSIONS"] = '10'
os.environ["CHROME_REFRESH_TIME"] = '600000'
os.environ["DEFAULT_BLOCK_ADS"] = 'true'
os.environ["DEFAULT_STEALTH"] = 'true'
os.environ["DEFAULT_IGNORE_HTTPS_ERRORS"] = 'true'

@pytest.fixture
def pdf_url():
    return "https://s21.q4cdn.com/861911615/files/doc_news/Arista-Networks-Inc.-Reports-Fourth-Quarter-and-Year-End-2024-Financial-Results-2025.pdf"

@pytest.fixture
def instance():
    return IRWorkflow({
        'deployment_type': 'local',
    })

@pytest.fixture
def default_prompt():
    return """
        # Metric Mapping Definition:
        #   Revenue -> revenue_billion
        #   Transaction Revenue -> transaction_revenue_billion
        #   Subscription Revenue -> subscription_revenue_billion
        #   Gross Profit -> gross_profit_billion
        #   Net Income -> net_income_billion
        #   Adjusted Ebitda -> adj_ebitda_billion
        #   Non Gaap Net Income -> non_gaap_net_income_billion
        #   Free Cash Flow -> free_cash_flow_billion
        #
        # All numerical values provided in millions should be converted to billions by dividing by 1000 and formatted to two decimal places.

        You will receive a body of text containing a company's financial report and historical financial metrics. Your task is to:

        1. **Extract Financial Metrics:**
        - If a metric is not mentioned in the document, it should be null. DO NOT MAKE UP METRICS IF THEY ARE NOT THERE.
        - Identify and extract every financial metric mentioned in the body of text using the example naming convention above, even if they are not listed above or the ones above are not present in the document.
        - Explicitly differentiate between **current quarter metrics** and **full year metrics** if both are present. For each metric, capture its value under either "current_quarter" or "full_year" in the output.
        - Additionally, extract any forward guidance metrics and differentiate them into:
                - **Next Quarter Forward Guidance**
                - **Fiscal Year Forward Guidance**
        For each forward guidance metric, if only a single value is provided, output a dictionary with the keys "low" and "high" both set to that value.
        - **Important:** When outputting any range values (such as forward guidance ranges), output a valid JSON object with two keys: "low" and "high", each mapped to the respective numeric value.
        - Convert large metric values (provided in millions) to billions format.

        2. **Compare Metrics:**
        - When historical data is available, compare each current quarter and full year metric with its corresponding historical metric.
        - Ensure that the keys match exactly. For example, if the report metric is "revenue_billion" under "current_quarter", compare it with the historical metric "current_revenue_billion".

        3. **Classify Sentiment:**
        - Identify any forward guidance statements or excerpts that may impact future performance.
        - Classify these excerpts as:
            - "Bullish" if they suggest growth, expansion, or an optimistic outlook.
            - "Bearish" if they imply contraction, risk, or a cautious tone.
            - "Neutral" if they are ambiguous or lack clear directional sentiment.
        - Include the exact text excerpts (snippets) that support each sentiment classification.

        4. **Output Structure:**
        - Produce the output as a JSON object with the following structure. For any metrics representing ranges (e.g., forward guidance), if only a single value is provided, output that value twice in a dictionary with keys "low" and "high".
        {
            "metrics": {
                "current_quarter": {
                    "<metric_key>": <value>,
                    ...
                },
                "full_year": {
                    "<metric_key>": <value>,
                    ...
                },
                "forward_guidance": {
                    "next_quarter": {
                        "<metric_key>_range": {"low": <lower>, "high": <upper>},
                        ...
                    },
                    "fiscal_year": {
                        "<metric_key>_range": {"low": <lower>, "high": <upper>},
                        ...
                    }
                }
            },
            "comparisons": {
                "current_quarter": {
                    "<metric_key>": "Current: $X vs Historical: $Y",
                    ...
                },
                "full_year": {
                    "<metric_key>": "Full Year: $X vs Historical: $Y",
                    ...
                }
            },
            "sentiment_snippets": [
                {"snippet": "Text excerpt here", "classification": "Bullish/Bearish/Neutral"}
            ]
        }

        5. **Highlight Context:**
        - Include the exact text excerpts as "snippets" from the report that support each sentiment classification.
        - Ignore standard legal language.

        **Output Requirement:**
        - The entire output must be valid JSON. Output the JSON in a code block (using triple backticks) to ensure proper formatting.
        """

@pytest.fixture
def hpe_test_config(default_prompt):
    return {
        "discord_webhook_url": os.environ.get("DISCORD_WEBHOOK_URL", ""),
        "groq_api_key": os.environ.get("GROQ_API_KEY", ""),
        "messages_table": os.environ.get('MESSAGES_TABLE', ''),
        'quarter': 1,
        "year": 2025,
        "browser_type": "firefox",
        "json_data": '{"current_revenue_billion": 161.71, "current_transaction_revenue_billion": 52.96, "current_subscription_revenue_billion": 108.75, "current_gross_profit_billion": 108.32, "current_net_income_billion": 12.85, "current_adj_ebitda_billion": 44.2, "current_non_gaap_net_income_billion": 32.6, "current_free_cash_flow_billion": 35.88, "full_year_revenue_billion": 681.88, "full_year_transaction_revenue_billion": 245.69, "full_year_subscription_revenue_billion": 436.19, "full_year_gross_profit_billion": 441.79, "full_year_net_income_billion": 29.96, "full_year_adj_ebitda_billion": 148.11, "full_year_non_gaap_net_income_billion": 99.45, "full_year_free_cash_flow_billion": 99.94, "next_quarter_sales_estimate_billion": 175.0, "fiscal_year_sales_estimate_billion": 179.0}',
        'deployment_type': 'local',
        'ticker': 'HPE',
        'base_url': 'https://www.hpe.com/us/en/newsroom/press-hub.html',
        'llm_instructions': {
            "system": default_prompt,
            "temperature": 0
        },
        "selector": "a.uc-card-wrapper",
        "url_ignore_list": [],
        "verify_keywords": {
            "fixed_terms": [
            "reports",
            "results"
            ],
            "quarter_as_string": True,
            "requires_quarter": True,
            "requires_year": True
        },
        'page_content_selector': 'body'
    }

def test_pdf_url(pdf_url, instance):
    text = instance.extract_pdf_text(pdf_url)
    search_text = "Revenue of $1.930 billion, an increase of 6.6% compared to the third quarter of 2024, and an increase of"
    assert search_text in text

@pytest.mark.asyncio
async def test_hpe_workflow(hpe_test_config):
    workflow = IRWorkflow(hpe_test_config)
    result = await workflow.process_earnings()
    print(result)