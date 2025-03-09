import os
import json
from typing import Any, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from groq import Groq
from openai import OpenAI
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sklearn.metrics.pairwise import cosine_similarity

load_dotenv()

def ask_groq(query: str, system: str, idx = None) -> Dict[str, Any]:
    client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": query}
        ],
        temperature=0,
        response_format={"type": "json_object"}
    )
    content: str = response.choices[0].message.content
    metrics: Dict[str, Any] = json.loads(content)
    if idx is not None:
        return idx, metrics
    return metrics

def parallel_ask_groq(calls, max_workers: int = 5) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(ask_groq, query, system, idx) for idx, _, query, system in calls]
        for future in as_completed(futures):
            results.append(future.result())
    return results

def embed_paragraphs(paragraphs: list[str]) -> list[list[float]]:
    client = OpenAI(api_key = os.environ.get("OPENAI_API_KEY", ""))
    embeddings = client.embeddings.create(
        input=paragraphs,
        model="text-embedding-3-small"
    )
    return [e.embedding for e in embeddings.data]

def sequential_semantic_clustering(paragraphs: List[str], threshold: float = 0.8) -> Dict[int, List[int]]:
    embeddings = embed_paragraphs(paragraphs)  # embeddings via OpenAI or local model
    
    clusters = {}
    cluster_idx = 0
    clusters[cluster_idx] = [0]  # start first cluster with paragraph 0

    for i in range(1, len(paragraphs)):
        sim = cosine_similarity(
            [embeddings[i]], [embeddings[i - 1]]
        )[0][0]

        if sim >= threshold:
            clusters[cluster_idx].append(i)
        else:
            cluster_idx += 1
            clusters[cluster_idx] = [i]

    return clusters

def group_non_whitespace_lines(lines):
    groups = []
    current_group = []

    for line in lines:
        # Check if the line is only whitespace characters (\xa0, \n, \t, spaces)
        if line.strip() in ["", "\xa0"]:
            if current_group:
                groups.append(current_group)
                current_group = []
        else:
            current_group.append(line.strip())

    # Add the last group if not empty
    if current_group:
        groups.append(current_group)

    return groups

def extract_soup(
    path,
    key_element_selector,
    key_element_class
):
    with open(path, 'r') as f:
        soup = BeautifulSoup(f, 'html5lib')
        
    ps = (
        soup
        .find(key_element_selector, attrs = {'class': key_element_class})
        .find_all(['p', 'li'])
    )

    filtered_paragraphs = [p for p in ps if not p.find_parent("table")]
    lines = [p.text for p in filtered_paragraphs]
    
    # Count lines that are purely whitespace characters
    whitespace_count = sum(1 for line in lines if line.strip() in ["", "\xa0"])
    
    if whitespace_count > 2:
        grouped = group_non_whitespace_lines(lines)
        return [' '.join(group) for group in grouped]

    return lines


if __name__ == '__main__':
    clustering_threshold = .75
    path = './output/2025-03-09/68b54982-3b62-4706-9dab-537bc9686304.html'
    quarter = 4
    year = 2024
    company = 'META'
    key_element_selector = 'div'
    key_element_class = 'xn-content'

    # path = './output/2025-03-09/f0dba033-054e-44fb-97d7-3b3233eecec3.html'
    # quarter = 1
    # year = 2025
    # company = 'HPE'
    # key_element_selector = 'div'
    # key_element_class = 'textOnly'
    
    paragraphs = extract_soup(
        path,
        key_element_selector = key_element_selector,
        key_element_class = key_element_class
    )

    clusters = sequential_semantic_clustering(
        paragraphs,
        threshold = clustering_threshold
    )

    calls = [
        (
            idx, 
            ' '.join([paragraphs[y] for y in x]),
            (
                f"the company is: {company}"
                f"the current fiscal year is: {year}"
                f"the current quarter is: {quarter}"
                f"next quarter is: Q{quarter+1 if quarter < 4 else 1} {year+1}"
                f"section: {' '.join([paragraphs[y] for y in x])}"
                f"return results in json like {'classification: <value_letter>'}"
            ),
            '''
            Given a section from an earnings release, 
            identify whether it is discussing:

            A. Current Quarter Results
            B. Both current quarter and fiscal year results
            C. Next Quarter Guidance
            D. Fiscal Year Guidance
            E. Both next quarter and fiscal year guidance
            F. General Highlights
            G. None of the above
            '''
        )
        for idx, x in enumerate(clusters.values())
    ]

    results = parallel_ask_groq(calls)

    df = (
        pd.DataFrame(results, columns = ['idx', 'classification'])
        .assign(
            classification = lambda df_: df_.classification.apply(lambda x: x.get('classification')),
            content = lambda df_: df_.idx.apply(lambda x: calls[x][1])
        )
        .groupby('classification').sum()
        .reset_index()
        .assign(
            classification = lambda df_: df_.classification.map({
                'A' : 'Current Quarter Results',
                'B' : 'Both current quarter and fiscal year results',
                'C': 'Next Quarter Guidance',
                'D': 'Fiscal Year Guidance',
                'E' : 'Both next quarter and fiscal year guidance',
                'F' : 'General Highlights',
                'G' : 'None of the above'
            })
        )
        .set_index('classification')
    )

    metrics = {}
    for classification in df.index:
        content = df.loc[classification, 'content']
        extracted_metrics = ask_groq(
            query = content,
            system = f'''
            You will receive a body of text containing a an excerpt 
            of {classification} metrics from  {company}'s Q{quarter} {year} press release.
            Your task is to identify and extract every financial metric
            mentioned in the body of text and return json like:
            ''' + '{<metric name>: <metric value>}'
        )
        if classification not in metrics:
            metrics[classification] = []

        metrics[classification].append(extracted_metrics)


    print(metrics)