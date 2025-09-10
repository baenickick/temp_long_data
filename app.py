# app.py
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import io

st.set_page_config(page_title="생활인구 CSV 병합 (특정 CODE 필터링)", layout="wide")
st.title("생활인구 CSV 병합 웹앱 (외국인 전용)")

def detect_delimiter(sample_bytes):
    s = sample_bytes[:2048].decode('utf-8', errors='ignore')
    return '\t' if s.count('\t') > s.count(',') else ','

def process_file(content_bytes, filename):
    delim = detect_delimiter(content_bytes)
    df = None
    for enc in ('utf-8', 'utf-8-sig', 'cp949', 'euc-kr'):
        try:
            df = pd.read_csv(
                io.BytesIO(content_bytes),
                encoding=enc,
                delimiter=delim,
                low_memory=False,
                dtype=str
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
    target_codes = ['1104065','1104066','1104067','1104068']
    mask = df['집계구코드_str'].str[:7].isin(target_codes)
    df = df[mask].copy()
    
    if df.empty:
        return pd.DataFrame()
    
    df['DATE'] = pd.to_datetime(df['기준일ID'], format='%Y%m%d', errors='coerce')
    df['TIME'] = pd.to_numeric(df['시간대구분'], errors='coerce')
    wmap = {0:'월요일',1:'화요일',2:'수요일',3:'목요일',4:'금요일',5:'토요일',6:'일요일'}
    df['요일'] = df['DATE'].dt.dayofweek.map(wmap)
    df['주중_or_주말'] = np.where(df['DATE'].dt.dayofweek >= 5, '주말', '주중')
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

st.info("📋 CODE 앞 7자리 필터링: 1104065, 1104066, 1104067, 1104068")

uploaded = st.file_uploader(
    "외국인 생활인구 CSV 파일 업로드 (여러 개 가능)", 
    type='csv', 
    accept_multiple_files=True
)

if uploaded:
    files_dict = {f.name: f.read() for f in uploaded}
    
    st.subheader("첫 번째 파일 5행 미리보기")
    first_name = list(files_dict)[0]
    sample = files_dict[first_name]
    delim = detect_delimiter(sample)
    for enc in ('utf-8','utf-8-sig','cp949','euc-kr'):
        try:
            df0 = pd.read_csv(io.BytesIO(sample), encoding=enc, delimiter=delim, nrows=5)
            df0.columns = df0.columns.str.strip().str.replace('"','').str.replace('?','')
            st.dataframe(df0)
            break
        except:
            continue

    st.markdown("---")
    
    if st.button("전체 실행"):
        progress = st.progress(0)
        dfs, errors = [], []
        total = len(files_dict)
        
        for i, (fname, content) in enumerate(files_dict.items(), start=1):
            try:
                dfp = process_file(content, fname)
                if not dfp.empty:
                    dfs.append(dfp)
                    st.write(f"✓ {fname}: {len(dfp):,}행")
                else:
                    st.write(f"○ {fname}: 0행")
            except Exception as e:
                errors.append(str(e))
                st.write(f"✗ {fname}: {e}")
            progress.progress(i / total)
        
        if dfs:
            merged = pd.concat(dfs, ignore_index=True).drop_duplicates()
            merged['DATE'] = pd.Categorical(merged['DATE'])
            merged = merged.sort_values(['DATE','TIME','CODE']).reset_index(drop=True)
            st.success(f"✅ 병합 완료: 총 {len(merged):,}행")
            
            bio = BytesIO()
            merged.to_excel(bio, index=False, engine='openpyxl')
            bio.seek(0)
            data = bio.getvalue()
            fn = f"foreigner_merged_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
            st.download_button(
                "엑셀 다운로드",
                data=data,
                file_name=fn,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        if errors:
            st.error("오류 발생:")
            for e in errors:
                st.write("-", e)
