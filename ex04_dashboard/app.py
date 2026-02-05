import streamlit as st
import matplotlib.pyplot as plt
from db import read_sql

st.set_page_config(page_title="NYC Yellow Taxi — Ex4", layout="wide")
st.title("NYC Yellow Taxi — Dashboard (Exercise 4)")

# ---- Test connexion ----
try:
    _ = read_sql("SELECT 1 AS ok;")
    st.sidebar.success("Connexion Postgres OK ✅")
except Exception as e:
    st.sidebar.error("Connexion Postgres KO ❌")
    st.sidebar.exception(e)
    st.stop()

st.sidebar.header("Filtres")
month = st.sidebar.text_input("Mois (YYYY-MM)", value="2025-01")

# Helper SQL : filtre mois sur tpep_pickup_datetime
# to_char(timestamp,'YYYY-MM') marche bien sur Postgres
params = {"month": month}

# ======================
# 1) KPIs globaux (mois)
# ======================
kpi_q = """
SELECT
  COUNT(*)                         AS nb_trips,
  AVG(total_amount)                AS avg_total,
  SUM(total_amount)                AS sum_total,
  AVG(trip_distance)               AS avg_distance,
  AVG(tip_amount)                  AS avg_tip
FROM fact_trips
WHERE to_char(tpep_pickup_datetime, 'YYYY-MM') = %(month)s;
"""
kpi = read_sql(kpi_q, params=params)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Nombre de courses", f"{int(kpi.loc[0,'nb_trips']):,}".replace(",", " "))
c2.metric("CA total", f"{float(kpi.loc[0,'sum_total'] or 0):,.2f}".replace(",", " "))
c3.metric("Panier moyen", f"{float(kpi.loc[0,'avg_total'] or 0):,.2f}".replace(",", " "))
c4.metric("Distance moyenne", f"{float(kpi.loc[0,'avg_distance'] or 0):,.2f}".replace(",", " "))
c5.metric("Tip moyen", f"{float(kpi.loc[0,'avg_tip'] or 0):,.2f}".replace(",", " "))

st.divider()

# ======================
# 2) CA par jour
# ======================
st.subheader("Chiffre d'affaires par jour")
daily_q = """
SELECT
  date(tpep_pickup_datetime) AS day,
  SUM(total_amount)          AS revenue
FROM fact_trips
WHERE to_char(tpep_pickup_datetime, 'YYYY-MM') = %(month)s
GROUP BY 1
ORDER BY 1;
"""
daily = read_sql(daily_q, params=params)

fig = plt.figure()
plt.plot(daily["day"], daily["revenue"])
plt.xticks(rotation=45)
plt.tight_layout()
st.pyplot(fig)

# ======================
# 3) Top 10 zones Pickup
# ======================
st.subheader("Top 10 zones de pickup (volume)")
top_pu_q = """
SELECT
  l.zone AS pickup_zone,
  l.borough AS borough,
  COUNT(*) AS trips
FROM fact_trips f
JOIN dim_location l ON l.location_id = f.pu_location_id
WHERE to_char(f.tpep_pickup_datetime, 'YYYY-MM') = %(month)s
GROUP BY 1, 2
ORDER BY trips DESC
LIMIT 10;
"""
top_pu = read_sql(top_pu_q, params=params)

fig = plt.figure()
plt.bar(top_pu["pickup_zone"], top_pu["trips"])
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
st.pyplot(fig)

# ======================
# 4) Répartition paiements
# ======================
st.subheader("Répartition des paiements")
pay_q = """
SELECT
  p.payment_name AS payment,
  COUNT(*)       AS trips
FROM fact_trips f
JOIN dim_payment_type p ON p.payment_type_id = f.payment_type_id
WHERE to_char(f.tpep_pickup_datetime, 'YYYY-MM') = %(month)s
GROUP BY 1
ORDER BY trips DESC;
"""
pay = read_sql(pay_q, params=params)

fig = plt.figure()
plt.pie(pay["trips"], labels=pay["payment"].astype(str), autopct="%1.1f%%")
plt.tight_layout()
st.pyplot(fig)

# ======================
# 5) CA par vendor
# ======================
st.subheader("Chiffre d'affaires par Vendor")
vendor_q = """
SELECT
  v.vendor_name AS vendor,
  SUM(f.total_amount) AS revenue,
  COUNT(*) AS trips
FROM fact_trips f
LEFT JOIN dim_vendor v ON v.vendor_id = f.vendor_id
WHERE to_char(f.tpep_pickup_datetime, 'YYYY-MM') = %(month)s
GROUP BY 1
ORDER BY revenue DESC;
"""
vendor = read_sql(vendor_q, params=params)

fig = plt.figure()
plt.bar(vendor["vendor"].fillna("Unknown"), vendor["revenue"])
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
st.pyplot(fig)

# ======================
# 6) Table échantillon (debug)
# ======================
with st.expander("Voir un échantillon des trajets (50 lignes)"):
    sample_q = """
    SELECT
      f.trip_id,
      f.tpep_pickup_datetime,
      f.tpep_dropoff_datetime,
      f.passenger_count,
      f.trip_distance,
      f.total_amount,
      pu.zone AS pickup_zone,
      "do".zone AS dropoff_zone,
      p.payment_name,
      v.vendor_name,
      r.rate_description
    FROM fact_trips f
    LEFT JOIN dim_location pu ON pu.location_id = f.pu_location_id
    LEFT JOIN dim_location "do" ON "do".location_id = f.do_location_id
    LEFT JOIN dim_payment_type p ON p.payment_type_id = f.payment_type_id
    LEFT JOIN dim_vendor v ON v.vendor_id = f.vendor_id
    LEFT JOIN dim_rate_code r ON r.rate_code_id = f.rate_code_id
    WHERE to_char(f.tpep_pickup_datetime, 'YYYY-MM') = %(month)s
    ORDER BY f.tpep_pickup_datetime
    LIMIT 50;
    """
    sample = read_sql(sample_q, params=params)
    st.dataframe(sample)
