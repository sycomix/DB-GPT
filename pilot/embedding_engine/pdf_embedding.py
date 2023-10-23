#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import List, Optional

from langchain.document_loaders import PyPDFLoader
from langchain.schema import Document
from langchain.text_splitter import (
    SpacyTextSplitter,
    RecursiveCharacterTextSplitter,
    TextSplitter,
)

from pilot.embedding_engine import SourceEmbedding, register


class PDFEmbedding(SourceEmbedding):
    """pdf embedding for read pdf document."""

    def __init__(
        self,
        file_path,
        vector_store_config,
        source_reader: Optional = None,
        text_splitter: Optional[TextSplitter] = None,
    ):
        """Initialize pdf word path."""
        super().__init__(
            file_path, vector_store_config, source_reader=None, text_splitter=None
        )
        self.file_path = file_path
        self.vector_store_config = vector_store_config
        self.source_reader = source_reader or None
        self.text_splitter = text_splitter or None

    @register
    def read(self):
        """Load from pdf path."""
        if self.source_reader is None:
            self.source_reader = PyPDFLoader(self.file_path)
        if self.text_splitter is None:
            try:
                self.text_splitter = SpacyTextSplitter(
                    pipeline="zh_core_web_sm",
                    chunk_size=100,
                    chunk_overlap=100,
                )
            except Exception:
                self.text_splitter = RecursiveCharacterTextSplitter(
                    chunk_size=100, chunk_overlap=50
                )

        return self.source_reader.load_and_split(self.text_splitter)

    @register
    def data_process(self, documents: List[Document]):
        for i, d in enumerate(documents):
            documents[i].page_content = d.page_content.replace("\n", "")
        return documents
