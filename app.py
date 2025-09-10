# app.py
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime

st.set_page_config(page_title="생활인구 CSV 병합 (지역 선택)", layout="wide")
st.title("생활인구 CSV 병합 웹앱 (드롭다운 지역 선택)")

# 1) regions.csv 로드
@st.cache_data
def load_region_mapping():
    df = pd.read_csv('regions.csv', sep='\t', dtype=str)
    df['코드7자리'] = df['통계청행정동코드'].str[:7]
    return df

region_df = load_region_mapping()

def detect_delimiter(sample_bytes):
    s = sample_bytes[:2048].decode('utf-8', errors='ignore')
    return '\t' if s.count('\t') > s.count(',') else ','

def process_file(content_bytes, filename, selected_codes):
    delim = detect_delimiter(content_bytes)
    df = None
    for enc in ('utf-8','utf-8-sig','cp949','euc-kr'):
        try:
            df = pd.read_csv(
                BytesIO(content_bytes), encoding=enc,
                delimiter=delim, dtype=str, low_memory=False
            )
            break
        except:
            continue
    if df is None:
        raise ValueError(f"{filename}: 인코딩 실패")
    df.columns = df.columns.str.strip().str.replace('"','').str.replace('?','')
    required = [
        '기준일ID','시간대구분','집계구코드',
        '총생활인구수','중국인체류인구수','중국외외국인체류인구수'
    ]
    if not all(c in df.columns for c in required):
        missing = [c for c in required if c not in df.columns]
        raise ValueError(f"{filename}: 필수 컬럼 누락 - {missing}")
    df = df[required].copy()
    df['집계구코드_str'] = df['집계구코드'].astype(str)

    # 필터링: 선택된 코드 없으면 전체
    if selected_codes:
        mask = df['집계구코드_str'].str[:7].isin(selected_codes)
        df = df[mask].copy()

    df['DATE'] = pd.to_datetime(df['기준일ID'], format='%Y%m%d', errors='coerce')
    df['TIME'] = pd.to_numeric(df['시간대구분'], errors='coerce')
    wmap = {0:'월요일',1:'화요일',2:'수요일',3:'목요일',
            4:'금요일',5:'토요일',6:'일요일'}
    df['요일'] = df['DATE'].dt.dayofweek.map(wmap)
    df['주중_or_주말'] = np.where(df['DATE'].dt.dayofweek>=5,'주말','주중')
    df['CODE'] = df['집계구코드_str']

    return pd.DataFrame({
        'DATE': df['DATE'].dt.strftime('%Y-%m-%d'),
        '요일': df['요일'],
        '주중_or_주말': df['주중_or_주말'],
        'TIME': df['TIME'],
        'CODE': df['CODE'],
        'ALL': pd.to_numeric(df['총생활인구수'], errors='coerce'),
        'CHN': pd.to_numeric(df['중국인체류인구수'], errors='coerce'),
        'EXP_CHN': pd.to_numeric(df['중국외외국인체류인구수'], errors='coerce')
    }).dropna(subset=['DATE','TIME','CODE'])

# 2) UI: 드롭다운으로 구->동 선택
st.header("📍 서울 지역 선택")
districts = sorted(region_df['시군구명'].unique())
col1, col2 = st.columns([1,2])
with col1:
    sel_district = st.selectbox("🏢 시군구 선택", ["전체"]+districts)
if sel_district=="전체":
    dongs = sorted(region_df['행정동명'].unique())
else:
    dongs = sorted(region_df[region_df['시군구명']==sel_district]['행정동명'])
with col2:
    sel_dongs = st.multiselect("🏘️ 행정동 선택", dongs)

# 3) 선택된 코드 리스트
if sel_district=="전체":
    selected_codes=[]
else:
    selected_codes = region_df[
        region_df['행정동명'].isin(sel_dongs)
    ]['코드7자리'].tolist()

st.markdown("---")
st.info("필터 없으면 전체 처리, 선택하면 해당 동만 처리")

uploaded = st.file_uploader("외국인 생활인구 CSV 업로드", type='csv', accept_multiple_files=True)
if uploaded:
    files_dict={f.name:f.read() for f in uploaded}
    st.subheader("첫 파일 미리보기")
    sample=next(iter(files_dict.values()))
    delim=detect_delimiter(sample)
    for enc in ('utf-8','utf-8-sig','cp949','euc-kr'):
        try:
            df0=pd.read_csv(BytesIO(sample),encoding=enc,delimiter=delim,nrows=5)
            df0.columns=df0.columns.str.strip().str.replace('"','').str.replace('?','')
            st.dataframe(df0);break
        except:pass
    if st.button("🚀 실행"):
        progress=st.progress(0);dfs,errs=[],[];total=len(files_dict)
        for i,(nm,ct) in enumerate(files_dict.items(),1):
            try:
                dfp=process_file(ct,nm,selected_codes)
                dfs.append(dfp);st.write(f"✓{nm}:{len(dfp):,}행")
            except Exception as e:
                errs.append(str(e));st.write(f"✗{nm}:{e}")
            progress.progress(i/total)
        if dfs:
    merged = pd.concat(dfs, ignore_index=True).drop_duplicates()
    merged['DATE'] = pd.Categorical(merged['DATE'])
    merged = merged.sort_values(['DATE','TIME','CODE']).reset_index(drop=True)
    st.success(f"완료: {len(merged):,}행")

    # 📥 엑셀 생성 및 다운로드
    bio = BytesIO()
    # 엑셀로 쓰기
    merged.to_excel(bio, index=False, engine='openpyxl')
    # 버퍼 시작으로 이동
    bio.seek(0)
    data = bio.getvalue()   # 전체 바이트 추출

    fn = f"merged_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    st.download_button(
        "다운로드",
        data=data,
        file_name=fn,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
