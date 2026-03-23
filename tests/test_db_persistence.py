import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from backend.app.infrastructure.db.db_dedup import upsert_document_with_lines_idempotent

@pytest.fixture
def mock_session():
    session = MagicMock()
    nested_mock = MagicMock()
    nested_mock.__enter__.return_value = nested_mock
    session.begin_nested.return_value = nested_mock
    return session

@patch('backend.app.infrastructure.db.db_dedup._find_existing')
@patch('backend.app.infrastructure.db.db_dedup.upsert_document_with_lines')
def test_idempotent_loading_ignores_duplicate_document(mock_upsert, mock_find, mock_session):
    mock_doc = MagicMock()
    mock_doc.id = 999
    mock_find.return_value = mock_doc
    
    md_doc = {"CUFE": "123", "CUDE": "", "DocumentID": "FV-1"}
    doc_id, is_duplicate = upsert_document_with_lines_idempotent(
        mock_session, 1, md_doc, {}, [], tenant_id="t1"
    )
    
    assert doc_id == 999
    assert is_duplicate is True
    mock_upsert.assert_not_called()
    mock_session.begin_nested.assert_not_called()

@patch('backend.app.infrastructure.db.db_dedup._find_existing')
@patch('backend.app.infrastructure.db.db_dedup.upsert_document_with_lines')
def test_savepoint_rollback_on_integrity_error(mock_upsert, mock_find, mock_session):
    mock_find.side_effect = [None, MagicMock(id=888)]
    mock_upsert.side_effect = IntegrityError("statement", "params", "orig")
    
    md_doc = {"CUFE": "dup_cufe", "DocumentID": "FV-2"}
    
    doc_id, is_duplicate = upsert_document_with_lines_idempotent(
        mock_session, 1, md_doc, {}, [], tenant_id="t1"
    )
    
    mock_session.begin_nested.assert_called_once()
    mock_session.rollback.assert_called_once()
    
    assert doc_id == 888
    assert is_duplicate is True

@patch('backend.app.infrastructure.db.db_dedup._find_existing')
@patch('backend.app.infrastructure.db.db_dedup.upsert_document_with_lines')
def test_savepoint_rollback_on_other_sqlalchemy_error(mock_upsert, mock_find, mock_session):
    mock_find.return_value = None
    mock_upsert.side_effect = SQLAlchemyError("Generic DB failure")
    
    md_doc = {"CUFE": "fail_cufe", "DocumentID": "FV-3"}
    
    with pytest.raises(SQLAlchemyError, match="Generic DB failure"):
        upsert_document_with_lines_idempotent(
            mock_session, 1, md_doc, {}, [], tenant_id="t1"
        )
        
    mock_session.begin_nested.assert_called_once()
    mock_session.rollback.assert_called_once()
