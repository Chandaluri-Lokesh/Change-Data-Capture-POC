"""
Pytest configuration: adds src/ to sys.path so all test imports resolve correctly.
"""
import os
import sys

_src = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if _src not in sys.path:
    sys.path.insert(0, _src)
