# app.py
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import io
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="생활인구 데이터 병합", layout="wide")
st.title("생활인구 CSV 파일 미리보기 & 병합")

def detect_delimiter(sample_bytes):
    s = sample_bytes[:2048].decode('utf-8', errors='ignore')
    return '\t' if s.count('\t') > s.count(',') else ','

def process_file(content):
    delim = detect_delimiter(content)
    for enc in ('utf-8','cp949','utf-8-sig'):
        try:
            df = pd.read_csv(io.BytesIO(content),
                             encoding=enc, delimiter=delim,
                             low_memory=False, dtype=str)
            break
        except:
            continue
    df.columns = df.columns.str.strip().str.replace('"','').str.replace('?','')
    req = ['기준일ID','시간대구분','집계구코드','총생활인구수',
           '중국인체류인구수','중국외외국인체류인구수']
    df = df[req].copy()
    df['DATE'] = pd.to_datetime(df['기준일ID'].astype(str),
                                format='%Y%m%d', errors='coerce')
    df['TIME'] = pd.to_numeric(df['시간대구분'], errors='coerce')
    wmap = {0:'월요일',1:'화요일',2:'수요일',3:'목요일',
            4:'금요일',5:'토요일',6:'일요일'}
    df['요일'] = df['DATE'].dt.dayofweek.map(wmap)
    df['주중_or_주말'] = np.where(df['DATE'].dt.dayofweek>=5,'주말','주중')
    df['CODE'] = (pd.to_numeric(df['집계구코드'], errors='coerce')
                  .astype('Int64').astype(str))
    result = pd.DataFrame({
        'DATE': df['DATE'].dt.strftime('%Y-%m-%d'),
        '요일': df['요일'],
        '주중_or_주말': df['주중_or_주말'],
        'TIME': df['TIME'],
        'CODE': df['CODE'],
        'ALL': pd.to_numeric(df['총생활인구수'], errors='coerce'),
        'CHN': pd.to_numeric(df['중국인체류인구수'], errors='coerce'),
        'EXP_CHN': pd.to_numeric(df['중국외외국인체류인구수'], errors='coerce')
    }).dropna(subset=['DATE','TIME','CODE'])
    return result

# 1) 파일 업로드
uploaded = st.file_uploader(
    "CSV 파일 업로드 (여러 개 선택 가능)", 
    type='csv', accept_multiple_files=True
)

if uploaded:
    # 2) 미리보기: 업로드된 파일 전체를 처리하지 않고, 병합 결과 상위 5행만 보여줌
    st.subheader("병합 후 미리보기 (상위 5행)")
    try:
        # 병렬로 각 파일 처리
        with ThreadPoolExecutor(max_workers=4) as ex:
            dfs = list(ex.map(lambda f: process_file(f.read()), uploaded))
        merged_preview = pd.concat(dfs, ignore_index=True).drop_duplicates()
        merged_preview['DATE'] = pd.Categorical(merged_preview['DATE'])
        merged_preview = merged_preview.sort_values(['DATE','TIME','CODE'])
        st.dataframe(merged_preview.head(5))
    except Exception as e:
        st.error(f"미리보기 처리 오류: {e}")

    st.markdown("---")
    # 3) 전체 실행 버튼
    if st.button("전체 실행"):
        n = len(uploaded)
        progress = st.progress(0)
        dfs, errors = [], []
        step = 0

        with ThreadPoolExecutor(max_workers=4) as ex:
            futures = {ex.submit(process_file, f.read()): f.name for f in uploaded}
            for fut in as_completed(futures):
                name = futures[fut]
                try:
                    df2 = fut.result()
                    dfs.append(df2)
                    st.write(f"✓ {name} 처리 완료 ({len(df2):,}행)")
                except Exception as e:
                    errors.append(f"{name}: {e}")
                    st.write(f"✗ {name} 오류: {e}")
                step += 1
                progress.progress(step / n)

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
            st.download_button(
                "엑셀 다운로드", 
                data=bio,
                file_name=fn,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        if errors:
            st.error("다음 오류가 발생했습니다:")
            for e in errors:
                st.write("-", e)
