import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "services", "worker"))
from services.worker.classes.ir import IRWorkflow

@pytest.fixture
def pdf_url():
    return "https://s21.q4cdn.com/861911615/files/doc_news/Arista-Networks-Inc.-Reports-Fourth-Quarter-and-Year-End-2024-Financial-Results-2025.pdf"

@pytest.fixture
def instance():
    return IRWorkflow({
        'deployment_type': 'local',
    })

def test_pdf_url(pdf_url, instance):
    text = instance.extract_pdf_text(pdf_url)
    print(text)
    search_text = "Revenue of $1.930 billion, an increase of 6.6% compared to the third quarter of 2024, and an increase of"
    assert search_text in text