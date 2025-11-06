# 1) Create & activate a venv (recommended)
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 2) Install deps
pip install -r requirements.txt

# 3) (Optional) set defaults
cp .env.example .env
# edit .env with your SMTP details

# 4) Launch
streamlit run streamlit_app.py

2ZVS8XBE6J6CB1227M9HJ78Z
