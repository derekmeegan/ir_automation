import os
import sys
import json
from typing import Any, Dict
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "serverless", "worker"))
from serverless.worker.handler import process

if __name__ == "__main__":
    # Set up environment variables for local testing.

    os.environ["QUARTER"] = '3'
    os.environ["YEAR"] = '2024'
    os.environ["JSON_DATA"] = '{"current_revenue_billion": 161.71, "current_transaction_revenue_billion": 52.96, "current_subscription_revenue_billion": 108.75, "current_gross_profit_billion": 108.32, "current_net_income_billion": 12.85, "current_adj_ebitda_billion": 44.2, "current_non_gaap_net_income_billion": 32.6, "current_free_cash_flow_billion": 35.88, "full_year_revenue_billion": 681.88, "full_year_transaction_revenue_billion": 245.69, "full_year_subscription_revenue_billion": 436.19, "full_year_gross_profit_billion": 441.79, "full_year_net_income_billion": 29.96, "full_year_adj_ebitda_billion": 148.11, "full_year_non_gaap_net_income_billion": 99.45, "full_year_free_cash_flow_billion": 99.94, "next_quarter_sales_estimate_billion": 175.0, "fiscal_year_sales_estimate_billion": 179.0}'
    os.environ["DEPLOYMENT_TYPE"] = 'local'
    os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY")
    os.environ["DISCORD_WEBHOOK_URL"] = os.getenv("DISCORD_WEBHOOK_URL")
    os.environ['MESSAGES_TABLE'] = os.getenv('MESSAGES_TABLE')
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
    # os.environ["SITE_CONFIG"] = json.dumps({
    #     "ticker":"ANET",
    #     "base_url": "https://investors.arista.com/Communications/Press-Releases-and-Events/default.aspx",
    #     "link_template": "https://s21.q4cdn.com/861911615/files/doc_news/Arista-Networks-Inc.-Reports-{quarter}-Quarter-{year}-Financial-Results-{current_year}.pdf",
    #     "url_keywords": {
    #         "requires_year": True,
    #         "requires_current_year": True, 
    #         "requires_quarter": True,
    #         "quarter_as_string": True,
    #         "quarter_is_title_case": True,
    #         "fixed_terms": ['-quarter']
    #     },
    #     "selectors": ["a.evergreen-news-attachment-PDF"],
    #     "key_phrase": "Download",
    #     "refine_link_list": True,
    #     "verify_keywords": {
    #         "requires_year": True,
    #         "requires_quarter": True,
    #         "quarter_as_string": True,
    #         "fixed_terms": ['-quarter']
    #     },
    #     "extraction_method": 'pdf',
    #     "custom_pdf_edit": None,  # or a function that modifies the PDF text
    #     "llm_instructions": {
    #         "system": """
    #         You will receive a body of text containing a company's financial report and historical financial metrics. Your task is to:

    #         1. **Extract Key Financial Metrics:**
    #         - Revenue (most recent quarter)
    #         - GAAP EPS (most recent quarter)
    #         - Non-GAAP EPS (most recent quarter)
    #         - Forward guidance for revenue and margins (if available)

    #         2. **Compare Metrics:**
    #         Provide these metrics in the following format:
    #         - "Revenue: $X billion"
    #         - "GAAP EPS: $X"
    #         - "Non-GAAP EPS: $X"
    #         - "Forward Guidance: Revenue: $X - $Y billion; Non-GAAP Gross Margin: X% - Y%"

    #         3. **Classify Sentiment:**
    #         - Identify forward guidance statements that could impact future performance.
    #         - Classify them as:
    #             - "Bullish" if they indicate growth, expansion, or optimistic outlook.
    #             - "Bearish" if they indicate contraction, risks, or cautious guidance.
    #             - "Neutral" if guidance is stable or lacks clear directional information.

    #         4. **Output Structure:**
    #         Produce the output as a JSON object with the following structure. If there are not ranges for forward guidance, provide the same number twice:
    #         {
    #             "metrics": {
    #             "revenue_billion": X.XX,
    #             "gaap_eps": X.XX,
    #             "non_gaap_eps": X.XX,
    #             "forward_guidance": {
    #                 "revenue_billion_range": [X.XX, Y.YY],
    #                 "non_gaap_gross_margin_range": [X, Y],
    #                 "non_gaap_operating_margin_range": [X, Y]
    #             }
    #             },
    #             "sentiment_snippets": [
    #             {"snippet": "Text excerpt here", "classification": "Bullish/Bearish/Neutral"}
    #             ]
    #         }

    #         5. **Highlight Context:**
    #         Include the exact text excerpts as "snippets" from the report that support each sentiment classification.

    #         Pass this JSON output to the Python function for comparison.
    #         """,
    #         "temperature": 0
    #     },
    #     "polling_config": {"interval": 2, "max_attempts": 30}
    # })
    # os.environ["SITE_CONFIG"] = json.dumps({
    #     "ticker":"TOST",
    #     "base_url": "https://investors.toasttab.com/news/default.aspx",
    #     "link_template": "https://investors.toasttab.com/news/news-details/{current_year}/Toast-Announces-{quarter}-Quarter-and-Full-Year-{year}-Financial-Results/default.aspx",
    #     "url_keywords": {
    #         "requires_year": True,
    #         "requires_current_year": True, 
    #         "requires_quarter": True,
    #         "quarter_as_string": True,
    #         "quarter_is_title_case": True,
    #         "fixed_terms": ['-Quarter']
    #     },
    #     "selectors": ["a.evergreen-news-headline-link"],
    #     "key_phrase": "Toast Announces",
    #     "refine_link_list": True,
    #     "verify_keywords": {
    #         "requires_year": True,
    #         "requires_quarter": True,
    #         "quarter_as_string": True,
    #         "fixed_terms": ['-quarter']
    #     },
    #     "custom_pdf_edit": None,  # or a function that modifies the PDF text
    #     "llm_instructions": {
    #         "system": """
    #         You will receive a body of text containing a company's financial report and historical financial metrics. Your task is to:

    #         1. **Extract Key Financial Metrics:**
    #         - Revenue (most recent quarter)
    #         - GAAP EPS (most recent quarter)
    #         - Non-GAAP EPS (most recent quarter)
    #         - Forward guidance for revenue and margins (if available)

    #         2. **Compare Metrics:**
    #         Provide these metrics in the following format:
    #         - "Revenue: $X billion"
    #         - "GAAP EPS: $X"
    #         - "Non-GAAP EPS: $X"
    #         - "Forward Guidance: Revenue: $X - $Y billion; Non-GAAP Gross Margin: X% - Y%"

    #         3. **Classify Sentiment:**
    #         - Identify forward guidance statements that could impact future performance.
    #         - Classify them as:
    #             - "Bullish" if they indicate growth, expansion, or optimistic outlook.
    #             - "Bearish" if they indicate contraction, risks, or cautious guidance.
    #             - "Neutral" if guidance is stable or lacks clear directional information.

    #         4. **Output Structure:**
    #         Produce the output as a JSON object with the following structure. If there are not ranges for forward guidance, provide the same number twice:
    #         {
    #             "metrics": {
    #             "revenue_billion": X.XX,
    #             "gaap_eps": X.XX,
    #             "non_gaap_eps": X.XX,
    #             "forward_guidance": {
    #                 "revenue_billion_range": [X.XX, Y.YY],
    #                 "non_gaap_gross_margin_range": [X, Y],
    #                 "non_gaap_operating_margin_range": [X, Y]
    #             }
    #             },
    #             "sentiment_snippets": [
    #             {"snippet": "Text excerpt here", "classification": "Bullish/Bearish/Neutral"}
    #             ]
    #         }

    #         5. **Highlight Context:**
    #         Include the exact text excerpts as "snippets" from the report that support each sentiment classification.

    #         Pass this JSON output to the Python function for comparison.
    #         """,
    #         "temperature": 0
    #     },
    #     "polling_config": {"interval": 2, "max_attempts": 30}
    # })
    # os.environ["SITE_CONFIG"] = json.dumps({
    #     "ticker":"XYZ",
    #     "base_url": "https://investors.block.xyz/financials/quarterly-earnings-reports/default.aspx",
    #     "selectors": ["a.module_link"],
    #     "key_phrase": "Shareholder Letter",
    #     "refine_link_list": True,
    #     "verify_keywords": {
    #         "requires_year": True,
    #         "requires_quarter": True,
    #         "quarter_with_q": True,
    #         "fixed_terms": []
    #     },
    #     "extraction_method": "pdf",
    #     "custom_pdf_edit": None,  # or a function that modifies the PDF text
    #     "llm_instructions": {
    #         "system": """
    #         You will receive a body of text containing a company's financial report and historical financial metrics. Your task is to:

    #         1. **Extract Key Financial Metrics:**
    #         - Revenue (most recent quarter)
    #         - GAAP EPS (most recent quarter)
    #         - Non-GAAP EPS (most recent quarter)
    #         - Forward guidance for revenue and margins (if available)

    #         2. **Compare Metrics:**
    #         Provide these metrics in the following format:
    #         - "Revenue: $X billion"
    #         - "GAAP EPS: $X"
    #         - "Non-GAAP EPS: $X"
    #         - "Forward Guidance: Revenue: $X - $Y billion; Non-GAAP Gross Margin: X% - Y%"

    #         3. **Classify Sentiment:**
    #         - Identify forward guidance statements that could impact future performance.
    #         - Classify them as:
    #             - "Bullish" if they indicate growth, expansion, or optimistic outlook.
    #             - "Bearish" if they indicate contraction, risks, or cautious guidance.
    #             - "Neutral" if guidance is stable or lacks clear directional information.

    #         4. **Output Structure:**
    #         Produce the output as a JSON object with the following structure. If there are not ranges for forward guidance, provide the same number twice:
    #         {
    #             "metrics": {
    #             "revenue_billion": X.XX,
    #             "gaap_eps": X.XX,
    #             "non_gaap_eps": X.XX,
    #             "forward_guidance": {
    #                 "revenue_billion_range": [X.XX, Y.YY],
    #                 "non_gaap_gross_margin_range": [X, Y],
    #                 "non_gaap_operating_margin_range": [X, Y]
    #             }
    #             },
    #             "sentiment_snippets": [
    #             {"snippet": "Text excerpt here", "classification": "Bullish/Bearish/Neutral"}
    #             ]
    #         }

    #         5. **Highlight Context:**
    #         Include the exact text excerpts as "snippets" from the report that support each sentiment classification.

    #         Pass this JSON output to the Python function for comparison.
    #         """,
    #         "temperature": 0
    #     },
    #     "polling_config": {"interval": 2, "max_attempts": 30}
    # })

    os.environ["SITE_CONFIG"] = json.dumps({
        "ticker": "HPE",
        "browser_type": "firefox",
        "base_url": "https://www.hpe.com/us/en/newsroom/press-hub.html",
        "key_phrase": "",
        "llm_instructions": {
        "system": """
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

        """,
        "temperature": 0
        },
        "polling_config": {
        "interval": 2,
        "max_attempts": 30
        },
        "refine_link_list": True,
        "selectors": "a.uc-card-wrapper",
        # "extraction_method": 'pdf',
        "url_ignore_list": [
           
        ],
        "verify_keywords": {
        "fixed_terms": [
           "reports",
           "results"
        ],
        "quarter_as_string": True,
        "requires_quarter": True,
        "requires_year": True
        },
        "href_ignore_words": ['Fiscal-Year-2023', 'Fiscal-Year-2022', 'Fiscal-Year-2021', 'Fiscal-Year-2020', 'Fiscal-Year-2019', 'Fiscal-Year-2018', 'Fiscal-Year-2017', 'Fiscal-Year-2016', 'Fiscal-Year-2015', 'Fiscal-Year-2014', 'Fiscal-Year-2013', 'Fiscal-Year-2012', 'Fiscal-Year-2011', 'Fiscal-Year-2010'],
        'page_content_selector': 'body'
    })

    result = process()
