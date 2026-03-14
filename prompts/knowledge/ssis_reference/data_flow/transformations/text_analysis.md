# SSIS Text Analysis Transformations - Knowledge Base

This document provides conversion patterns for SSIS Text Analysis transformations.

## 1. Term Extraction
Extracts nouns or noun phrases from text data.

**PySpark Equivalent**:
Requires NLP libraries like `spark-nlp` or specialized UDFs using `nltk` or `spacy`.
```python
# Example using Spark NLP (requires setup)
from sparknlp.base import *
from sparknlp.annotator import *

# Define NLP pipeline to extract nouns
```

## 2. Term Lookup
Matches terms in a text column against a reference table of terms.

**PySpark Equivalent**:
Can be implemented using string matching or regular expressions.
```python
# Simple contains match
matched_df = df.join(terms_df, df.text.contains(terms_df.term))
```
