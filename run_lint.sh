#!/usr/bin/env zsh

echo "A flake8----------"
flake8 *.py */*.py --count --select=E9,F36,F92 --show-source --max-line-length=100 --statistics
echo "\nB flake8 ----------"
flake8 *.py */*.py --count --max-complexity=10 --max-line-length=100 --statistics
echo "\nC pylint----------"
pylint --max-locals=25  *.py */*.py
