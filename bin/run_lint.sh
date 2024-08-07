#!/usr/bin/env zsh

if False; then
echo "A flake8----------"
    flake8 *.py  --count --select=E9,F36,F92 --show-source --max-line-length=100 --statistics
    flake8 util/*.py --count --select=E9,F36,F92 --show-source --max-line-length=100 --statistics
    flake8 tools/*.py --count --select=E9,F36,F92 --show-source --max-line-length=100 --statistics
fi

if True; then
echo "\nB flake8 ----------"
    flake8 *.py --count --max-complexity=10 --max-line-length=100 --statistics
    flake8 util/*.py --count --max-complexity=10 --max-line-length=100 --statistics
    flake8 tools/*.py --count --max-complexity=10 --max-line-length=100 --statistics
echo "\nC pylint----------"
fi

if False; then
    pylint --max-locals=25  *.py */*.py
fi
