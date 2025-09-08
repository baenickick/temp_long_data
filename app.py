# app.py
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import io

st.set_page_config(page_title="생활인구 데이터 병합", layout="wide")
st.title("생활인구 CSV 병합 웹앱 (외국인/국내인구 자동 구분)")

def detect_delimiter(sample_bytes):
    s = sample_bytes[:2048].decode('utf-8', errors='ignore')
    return '\t' if s.count('\t') > s.count(',') else ','

def process_foreigner_file(content_bytes, filename):
    """외국인 파일 처리"""
    delim = detect_delimiter(content_bytes)
    df = None
    for enc in ('utf-8', 'cp949', 'utf-8-sig'):
        try:
            df = pd.read_csv(io.BytesIO(content_bytes), encoding=enc, delimiter=delim, low_memory=False, dtype=str)
            break
        except Exception:
            continue
    if df is None:
        raise ValueError(f"{filename}: 인코딩 실패")
    
    df.columns = df.columns.str.strip().str.replace('"','').str.replace('?','')
    required = ['기준일ID','시간대구분','집계구코드','총생활인구수','중국인체류인구수','중국외외국인체류인구수']
    if not all(c in df.columns for c in required):
        raise ValueError(f"{filename}: 필수 컬럼 누락")
    
    df = df[required].copy()
    target_codes = ['1104065', '1104066', '1104067', '1104068']
    df['집계구코드_str'] = df['집계구코드'].astype(str)
    filter_mask = df['집계구코드_str'].str[:7].isin(target_codes)
    df = df[filter_mask].copy()
    
    if len(df) == 0:
        return pd.DataFrame()
    
    df['DATE'] = pd.to_datetime(df['기준일ID'].astype(str), format='%Y%m%d', errors='coerce')
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

def process_local_file(content_bytes, filename):
    """국내인구 파일 처리"""
    delim = detect_delimiter(content_bytes)
    df = None
    for enc in ('utf-8', 'cp949', 'utf-8-sig'):
        try:
            df = pd.read_csv(io.BytesIO(content_bytes), encoding=enc, delimiter=delim, low_memory=False, dtype=str)
            break
        except Exception:
            continue
    if df is None:
        raise ValueError(f"{filename}: 인코딩 실패")
    
    df.columns = df.columns.str.strip().str.replace('"','').str.replace('?','')
    
    basic_cols = ['기준일ID','시간대구분','행정동코드','집계구코드','총생활인구수']
    male_cols = [
        '남자0세부터9세생활인구수','남자10세부터14세생활인구수','남자15세부터19세생활인구수',
        '남자20세부터24세생활인구수','남자25세부터29세생활인구수','남자30세부터34세생활인구수',
        '남자35세부터39세생활인구수','남자40세부터44세생활인구수','남자45세부터49세생활인구수',
        '남자50세부터54세생활인구수','남자55세부터59세생활인구수','남자60세부터64세생활인구수',
        '남자65세부터69세생활인구수','남자70세이상생활인구수'
    ]
    female_cols = [
        '여자0세부터9세생활인구수','여자10세부터14세생활인구수','여자15세부터19세생활인구수',
        '여자20세부터24세생활인구수','여자25세부터29세생활인구수','여자30세부터34세생활인구수',
        '여자35세부터39세생활인구수','여자40세부터44세생활인구수','여자45세부터49세생활인구수',
        '여자50세부터54세생활인구수','여자55세부터59세생활인구수','여자60세부터64세생활인구수',
        '여자65세부터69세생활인구수','여자70세이상생활인구수'
    ]
    
    required = basic_cols + male_cols + female_cols
    if not all(c in df.columns for c in required):
        raise ValueError(f"{filename}: 필수 컬럼 누락")
    
    df = df[required].copy()
    target_codes = ['1104065', '1104066', '1104067', '1104068']
    df['집계구코드_str'] = df['집계구코드'].astype(str)
    filter_mask = df['집계구코드_str'].str[:7].isin(target_codes)
    df = df[filter_mask].copy()
    
    if len(df) == 0:
        return pd.DataFrame()
    
    df['DATE'] = pd.to_datetime(df['기준일ID'].astype(str), format='%Y%m%d', errors='coerce')
    df['TIME'] = pd.to_numeric(df['시간대구분'], errors='coerce')
    wmap = {0:'월요일',1:'화요일',2:'수요일',3:'목요일',4:'금요일',5:'토요일',6:'일요일'}
    df['요일'] = df['DATE'].dt.dayofweek.map(wmap)
    df['주중_or_주말'] = np.where(df['DATE'].dt.dayofweek >= 5, '주말', '주중')
    df['CODE'] = df['집계구코드_str']
    
    def to_numeric_safe(col):
        return pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    male_total = sum(to_numeric_safe(col) for col in male_cols)
    female_total = sum(to_numeric_safe(col) for col in female_cols)
    
    return pd.DataFrame({
        'DATE': df['DATE'].dt.strftime('%Y-%m-%d'),
        '요일': df['요일'],
        '주중_or_주말': df['주중_or_주말'],
        'TIME': df['TIME'],
        'CODE': df['CODE'],
        '남자': male_total,
        '여자': female_total,
        '남자 0-9': to_numeric_safe('남자0세부터9세생활인구수'),
        '남자 10-19': to_numeric_safe('남자10세부터14세생활인구수') + to_numeric_safe('남자15세부터19세생활인구수'),
        '남자 20-29': to_numeric_safe('남자20세부터24세생활인구수') + to_numeric_safe('남자25세부터29세생활인구수'),
        '남자 30-39': to_numeric_safe('남자30세부터34세생활인구수') + to_numeric_safe('남자35세부터39세생활인구수'),
        '남자 40-49': to_numeric_safe('남자40세부터44세생활인구수') + to_numeric_safe('남자45세부터49세생활인구수'),
        '남자 50-59': to_numeric_safe('남자50세부터54세생활인구수') + to_numeric_safe('남자55세부터59세생활인구수'),
        '남자 60-69': to_numeric_safe('남자60세부터64세생활인구수') + to_numeric_safe('남자65세부터69세생활인구수'),
        '남자 70+': to_numeric_safe('남자70세이상생활인구수'),
        '여자 0-9': to_numeric_safe('여자0세부터9세생활인구수'),
        '여자 10-19': to_numeric_safe('여자10세부터14세생활인구수') + to_numeric_safe('여자15세부터19세생활인구수'),
        '여자 20-29': to_numeric_safe('여자20세부터24세생활인구수') + to_numeric_safe('여자25세부터29세생활인구수'),
        '여자 30-39': to_numeric_safe('여자30세부터34세생활인구수') + to_numeric_safe('여자35세부터39세생활인구수'),
        '여자 40-49': to_numeric_safe('여자40세부터44세생활인구수') + to_numeric_safe('여자45세부터49세생활인구수'),
        '여자 50-59': to_numeric_safe('여자50세부터54세생활인구수') + to_numeric_safe('여자55세부터59세생활인구수'),
        '여자 60-69': to_numeric_safe('여자60세부터64세생활인구수') + to_numeric_safe('여자65세부터69세생활인구수'),
        '여자 70+': to_numeric_safe('여자70세이상생활인구수')
    }).dropna(subset=['DATE','TIME','CODE'])

st.info("📋 필터링 대상: CODE 앞 7자리가 1104065, 1104066, 1104067, 1104068인 데이터만 처리")
st.info("🔄 파일명에 따라 자동 구분: TEMP_FOREIGNER → 외국인, LOCAL_PEOPLE → 국내인구")

uploaded = st.file_uploader("CSV 파일을 업로드하세요 (여러 개 가능)", type='csv', accept_multiple_files=True)

if uploaded:
    file_bytes = {f.name: f.read() for f in uploaded}
    
    # 파일 분류
    foreigner_files = [f for f in uploaded if 'TEMP_FOREIGNER' in f.name or 'FOREIGNER' in f.name]
    local_files = [f for f in uploaded if 'LOCAL_PEOPLE' in f.name]
    other_files = [f for f in uploaded if f not in foreigner_files + local_files]
    
    st.subheader("업로드된 파일 분류")
    if foreigner_files:
        st.write(f"🌍 외국인 파일: {len(foreigner_files)}개")
    if local_files:
        st.write(f"🏠 국내인구 파일: {len(local_files)}개")
    if other_files:
        st.write(f"❓ 기타 파일: {len(other_files)}개")
    
    # 미리보기
    st.subheader("첫 번째 파일 미리보기")
    first_file = uploaded[0]
    sample = file_bytes[first_file.name]
    delim = detect_delimiter(sample)
    for enc in ('utf-8','cp949','utf-8-sig'):
        try:
            preview_df = pd.read_csv(io.BytesIO(sample), encoding=enc, delimiter=delim, nrows=5)
            preview_df.columns = preview_df.columns.str.strip().str.replace('"','').str.replace('?','')
            st.dataframe(preview_df)
            break
        except:
            continue

    st.markdown("---")

    if st.button("전체 실행"):
        progress = st.progress(0)
        all_dfs, errors = [], []
        total_files = len(file_bytes)
        
        for i, (fname, content) in enumerate(file_bytes.items(), start=1):
            try:
                if 'LOCAL_PEOPLE' in fname:
                    df = process_local_file(content, fname)
                    file_type = "국내인구"
                else:
                    df = process_foreigner_file(content, fname)
                    file_type = "외국인"
                
                if len(df) > 0:
                    all_dfs.append(df)
                    st.write(f"✓ {fname} ({file_type}) 처리 완료: {len(df):,}행")
                else:
                    st.write(f"○ {fname} ({file_type}) 처리 완료: 0행")
            except Exception as e:
                errors.append(f"{fname}: {e}")
                st.write(f"✗ {fname} 오류: {e}")
            progress.progress(i / total_files)

        if all_dfs:
            merged = pd.concat(all_dfs, ignore_index=True).drop_duplicates()
            merged['DATE'] = pd.Categorical(merged['DATE'])
            merged = merged.sort_values(['DATE','TIME','CODE']).reset_index(drop=True)
            st.success(f"✅ 통합 병합 완료: 총 {len(merged):,}행")
            
            bio = BytesIO()
            merged.to_excel(bio, index=False, engine='openpyxl')
            bio.seek(0)
            data_bytes = bio.getvalue()
            fn = f"integrated_merged_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
            st.download_button("통합 엑셀 다운로드", data=data_bytes, file_name=fn,
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.warning("⚠️ 처리 가능한 데이터가 없습니다.")
        
        if errors:
            st.error("오류 발생:")
            for e in errors:
                st.write("-", e)
