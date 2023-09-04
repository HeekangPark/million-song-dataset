#!/bin/bash

echo "alias ll='ls -alFh'" >> ~/.bashrc
echo "export PATH=\"\$HOME/.local/bin:\$PATH\"" >> ~/.bashrc
source ~/.bashrc

pip install jupyterlab ipywidgets findspark pandas plotly matplotlib scipy
pip uninstall -y urllib3
pip install urllib3==1.26.6

mkdir -p ~/workspace
