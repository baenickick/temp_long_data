# app.py
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import io

st.set_page_config(page_title="생활인구 데이터 병합", layout="wide")
st.title("생활인구 CSV 병합 웹앱")

def detect_delimiter(sample_bytes):
    s = sample_bytes[:2048].decode('utf-8', errors='ignore')
    return '\t' if s.count('\t') > s.count(',') else ','

def process_file(content):
    delim = detect_delimiter(content)
    for enc in ('utf-8', 'cp949', 'utf-8-sig'):
        try:
            df = pd.read_csv(
                io.BytesIO(content),
                encoding=enc,
                delimiter=delim,
                low_memory=False,
                dtype=str
            )
            break
        except:
            continue
    df.columns = df.columns.str.strip().str.replace('"','').str.replace('?','')
    req = [
        '기준일ID', '시간대구분', '집계구코드',
        '총생활인구수', '중국인체류인구수', '중국외외국인체류인구수'
    ]
    df = df[req].copy()
    df['DATE'] = pd.to_datetime(df['기준일ID'].astype(str), format='%Y%m%d', errors='coerce')
    df['TIME'] = pd.to_numeric(df['시간대구분'], errors='coerce')
    wmap = {
        0:'월요일',1:'화요일',2:'수요일',3:'목요일',
        4:'금요일',5:'토요일',6:'일요일'
    }
    df['요일'] = df['DATE'].dt.dayofweek.map(wmap)
    df['주중_or_주말'] = np.where(df['DATE'].dt.dayofweek >= 5, '주말', '주중')
    df['CODE'] = (
        pd.to_numeric(df['집계구코드'], errors='coerce')
          .astype('Int64')
          .astype(str)
    )
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

# 1) 파일 업로드
uploaded = st.file_uploader(
    "CSV 파일을 업로드하세요 (여러 개 가능)", 
    type='csv',
    accept_multiple_files=True
)

if uploaded:
    # 첫 번째 파일 5행 미리보기
    st.subheader("첫 번째 파일 5행 미리보기")
    first = uploaded[0]
    try:
        delim = detect_delimiter(first.read())
        for enc in ('utf-8','cp949','utf-8-sig'):
            try:
                preview = pd.read_csv(
                    io.BytesIO(first.read()),
                    encoding=enc,
                    delimiter=delim,
                    nrows=5
                )
                preview.columns = preview.columns.str.strip().str.replace('"','').str.replace('?','')
                st.dataframe(preview)
                break
            except:
                continue
    except Exception:
        st.write("미리보기 불가: 인코딩 또는 구분자 확인 필요")

    st.markdown("---")

    # 전체 실행 버튼
    if st.button("전체 실행"):
        st.subheader("전체 병합 진행")
        n = len(uploaded)
        progress = st.progress(0)
        dfs, errors = [], []

        for i, f in enumerate(uploaded, start=1):
            try:
                df2 = process_file(f.read())
                dfs.append(df2)
                st.write(f"✓ {f.name} 처리 완료 ({len(df2):,}행)")
            except Exception as e:
                errors.append(f"{f.name}: {e}")
                st.write(f"✗ {f.name} 오류: {e}")
            progress.progress(i / n)

        if dfs:
            merged = pd.concat(dfs, ignore_index=True).drop_duplicates()
            merged['DATE'] = pd.Categorical(merged['DATE'])
            merged = merged.sort_values(['DATE','TIME','CODE']).reset_index(drop=True)
            st.success(f"✅ 병합 완료: 총 {len(merged):,}행")

            # 다운로드
            bio = BytesIO()
            merged.to_excel(bio, index=False, engine='openpyxl')
            data_bytes = bio.getvalue()  # ← get raw bytes
            fn = f"merged_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
            st.download_button(
                "엑셀 다운로드",
                data=data_bytes,
                file_name=fn,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        if errors:
            st.error("다음 오류가 발생했습니다:")
            for e in errors:
                st.write("-", e)
