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
    assert text