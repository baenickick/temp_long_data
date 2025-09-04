# app.py
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import io, csv
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="생활인구 데이터 병합", layout="wide")
st.title("생활인구 CSV 파일 미리보기 & 병합")

# 유틸 함수들
def detect_delimiter(sample_bytes):
    s = sample_bytes[:2048].decode('utf-8', errors='ignore')
    return '\t' if s.count('\t') > s.count(',') else ','

def read_preview(content):
    delim = detect_delimiter(content)
    for enc in ('utf-8','cp949','utf-8-sig'):
        try:
            df = pd.read_csv(io.BytesIO(content),
                             encoding=enc, delimiter=delim,
                             nrows=5)
            df.columns = df.columns.str.strip().str.replace('"','').str.replace('?','')
            return df
        except:
            continue
    return None

def process_file(content, filename):
    delim = detect_delimiter(content)
    df = None
    for enc in ('utf-8','cp949','utf-8-sig'):
        try:
            df = pd.read_csv(io.BytesIO(content),
                             encoding=enc, delimiter=delim,
                             low_memory=False, dtype=str)
            break
        except:
            continue
    if df is None:
        raise ValueError(f"{filename}: 인코딩 오류")
    df.columns = df.columns.str.strip().str.replace('"','').str.replace('?','')
    req = ['기준일ID','시간대구분','집계구코드','총생활인구수',
           '중국인체류인구수','중국외외국인체류인구수']
    if not all(c in df.columns for c in req):
        raise ValueError(f"{filename}: 필수 컬럼 누락")
    d = df[req].copy()
    d['DATE'] = pd.to_datetime(d['기준일ID'].astype(str),format='%Y%m%d',errors='coerce')
    d['TIME'] = pd.to_numeric(d['시간대구분'],errors='coerce')
    wmap={0:'월요일',1:'화요일',2:'수요일',3:'목요일',
          4:'금요일',5:'토요일',6:'일요일'}
    d['요일'] = d['DATE'].dt.dayofweek.map(wmap)
    d['주중_or_주말'] = np.where(d['DATE'].dt.dayofweek>=5,'주말','주중')
    d['CODE'] = (pd.to_numeric(d['집계구코드'],errors='coerce')
                .astype('Int64').astype(str))
    df2=pd.DataFrame({
        'DATE': d['DATE'].dt.strftime('%Y-%m-%d'),
        '요일': d['요일'],
        '주중_or_주말': d['주중_or_주말'],
        'TIME': d['TIME'],
        'CODE': d['CODE'],
        'ALL': pd.to_numeric(d['총생활인구수'],errors='coerce'),
        'CHN': pd.to_numeric(d['중국인체류인구수'],errors='coerce'),
        'EXP_CHN': pd.to_numeric(d['중국외외국인체류인구수'],errors='coerce')
    }).dropna(subset=['DATE','TIME','CODE'])
    return df2

# 1) 파일 업로드
uploaded = st.file_uploader(
    "CSV 파일을 업로드하세요 (여러 개 선택 가능)", 
    type='csv', accept_multiple_files=True
)

if uploaded:
    st.subheader("파일별 5행 미리보기")
    for f in uploaded:
        st.markdown(f"**{f.name}**")
        preview = read_preview(f.read())
        if preview is not None:
            st.dataframe(preview)
        else:
            st.write("미리보기 실패: 인코딩 또는 구분자 확인 필요")
    st.markdown("---")
    if st.button("전체 실행"):
        n = len(uploaded)
        progress = st.progress(0)
        dfs = []
        errors = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            futures = {ex.submit(process_file, f.read(), f.name): f.name for f in uploaded}
            for i, fut in enumerate(as_completed(futures), start=1):
                name = futures[fut]
                try:
                    df2 = fut.result()
                    dfs.append(df2)
                    st.write(f"✓ {name} 처리 완료 ({len(df2):,}행)")
                except Exception as e:
                    errors.append(str(e))
                    st.write(f"✗ {name} 오류: {e}")
                progress.progress(i/n)
        if dfs:
            merged = pd.concat(dfs, ignore_index=True).drop_duplicates()
            merged['DATE'] = pd.Categorical(merged['DATE'])
            merged = merged.sort_values(['DATE','TIME','CODE']).reset_index(drop=True)
            st.success(f"✅ 병합 완료: 총 {len(merged):,}행")
            # 다운로드
            bio = BytesIO()
            merged.to_excel(bio, index=False, engine='openpyxl')
            bio.seek(0)
            fn = f"merged_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
            st.download_button("엑셀 다운로드", data=bio, file_name=fn,
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        if errors:
            st.error("다음 오류가 발생했습니다:")
            for e in errors:
                st.write("-", e)
