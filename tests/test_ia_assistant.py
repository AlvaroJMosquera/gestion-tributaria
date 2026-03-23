import pytest
from unittest.mock import patch, MagicMock
from backend.app.application.sql_assistant import SQLAssistant

@pytest.fixture
def assistant():
    # Instanciamos el asistente con un tenant dummy
    ast = SQLAssistant("test-tenant-123", model="gemini-2.5-flash", api_key="dummy")
    return ast

@patch('backend.app.application.sql_assistant.genai.Client')
def test_generate_sql_uses_gemini_and_enforces_tenant(mock_client_class, assistant):
    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    
    mock_response = MagicMock()
    mock_response.text = '{"sql": "SELECT SUM(total) FROM lines l"}'
    mock_client.models.generate_content.return_value = mock_response
    
    # Hacemos que check_internet pase
    with patch('urllib.request.urlopen'):
        sql = assistant.generate_sql("Cuánto he vendido?")
        
    assert "l.tenant_id = 'test-tenant-123'" in sql.lower()
    assert "sum(total) from lines l" in sql.lower()

@patch('urllib.request.urlopen')
def test_generate_sql_uses_ollama_when_explicit(mock_urlopen, assistant):
    assistant.model = "llama3.1"
    
    mock_response = MagicMock()
    mock_response.read.return_value = b'{"response": "{\\"sql\\":\\"SELECT * FROM documents d\\"}"}'
    mock_response.__enter__.return_value = mock_response
    mock_urlopen.return_value = mock_response
    
    sql = assistant.generate_sql("dame facturas")
    
    assert "d.tenant_id = 'test-tenant-123'" in sql.lower()
    assert "select * from documents d" in sql.lower()
