import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import io

st.set_page_config(page_title="대용량 생활인구 데이터 병합", layout="wide")
st.title("대용량 CSV 병합 (청크 처리)")

def detect_delimiter(sample_bytes):
    s = sample_bytes[:2048].decode('utf-8', errors='ignore')
    return '\t' if s.count('\t') > s.count(',') else ','

def process_file_chunks(content_bytes, filename, chunk_size=10000):
    """청크 단위로 대용량 파일 처리"""
    delim = detect_delimiter(content_bytes)
    
    # 인코딩 감지
    encoding = None
    for enc in ('utf-8', 'cp949', 'utf-8-sig'):
        try:
            pd.read_csv(io.BytesIO(content_bytes), encoding=enc, delimiter=delim, nrows=1)
            encoding = enc
            break
        except:
            continue
    
    if not encoding:
        raise ValueError(f"{filename}: 인코딩 실패")
    
    # 필터링 대상 코드
    target_codes = ['1104065', '1104066', '1104067', '1104068']
    processed_chunks = []
    
    # 청크 단위로 파일 읽기
    chunk_reader = pd.read_csv(
        io.BytesIO(content_bytes), 
        encoding=encoding, 
        delimiter=delim,
        chunksize=chunk_size,
        dtype=str,
        low_memory=False
    )
    
    chunk_count = 0
    for chunk in chunk_reader:
        chunk_count += 1
        
        # 컬럼 정리
        chunk.columns = chunk.columns.str.strip().str.replace('"','').str.replace('?','')
        
        # 외국인 파일 처리
        if 'FOREIGNER' in filename or 'TEMP_FOREIGNER' in filename:
            required = ['기준일ID','시간대구분','집계구코드','총생활인구수','중국인체류인구수','중국외외국인체류인구수']
            if all(c in chunk.columns for c in required):
                chunk_filtered = process_foreigner_chunk(chunk, required, target_codes)
                if len(chunk_filtered) > 0:
                    processed_chunks.append(chunk_filtered)
        
        # 국내인구 파일 처리
        elif 'LOCAL_PEOPLE' in filename:
            basic_cols = ['기준일ID','시간대구분','행정동코드','집계구코드','총생활인구수']
            male_cols = [f'남자{age}생활인구수' for age in ['0세부터9세','10세부터14세','15세부터19세','20세부터24세','25세부터29세','30세부터34세','35세부터39세','40세부터44세','45세부터49세','50세부터54세','55세부터59세','60세부터64세','65세부터69세','70세이상']]
            female_cols = [f'여자{age}생활인구수' for age in ['0세부터9세','10세부터14세','15세부터19세','20세부터24세','25세부터29세','30세부터34세','35세부터39세','40세부터44세','45세부터49세','50세부터54세','55세부터59세','60세부터64세','65세부터69세','70세이상']]
            
            if all(c in chunk.columns for c in basic_cols + male_cols + female_cols):
                chunk_filtered = process_local_chunk(chunk, basic_cols, male_cols, female_cols, target_codes)
                if len(chunk_filtered) > 0:
                    processed_chunks.append(chunk_filtered)
        
        # 진행 상황 표시
        if chunk_count % 10 == 0:
            st.write(f"  📊 청크 {chunk_count} 처리 중...")
    
    return processed_chunks

def process_foreigner_chunk(chunk, required, target_codes):
    chunk = chunk[required].copy()
    chunk['집계구코드_str'] = chunk['집계구코드'].astype(str)
    filter_mask = chunk['집계구코드_str'].str[:7].isin(target_codes)
    chunk = chunk[filter_mask]
    
    if len(chunk) == 0:
        return pd.DataFrame()
    
    chunk['DATE'] = pd.to_datetime(chunk['기준일ID'].astype(str), format='%Y%m%d', errors='coerce')
    chunk['TIME'] = pd.to_numeric(chunk['시간대구분'], errors='coerce')
    wmap = {0:'월요일',1:'화요일',2:'수요일',3:'목요일',4:'금요일',5:'토요일',6:'일요일'}
    chunk['요일'] = chunk['DATE'].dt.dayofweek.map(wmap)
    chunk['주중_or_주말'] = np.where(chunk['DATE'].dt.dayofweek >= 5, '주말', '주중')
    
    return pd.DataFrame({
        'DATE': chunk['DATE'].dt.strftime('%Y-%m-%d'),
        '요일': chunk['요일'],
        '주중_or_주말': chunk['주중_or_주말'],
        'TIME': chunk['TIME'],
        'CODE': chunk['집계구코드_str'],
        'ALL': pd.to_numeric(chunk['총생활인구수'], errors='coerce'),
        'CHN': pd.to_numeric(chunk['중국인체류인구수'], errors='coerce'),
        'EXP_CHN': pd.to_numeric(chunk['중국외외국인체류인구수'], errors='coerce')
    }).dropna(subset=['DATE','TIME','CODE'])

def process_local_chunk(chunk, basic_cols, male_cols, female_cols, target_codes):
    chunk = chunk[basic_cols + male_cols + female_cols].copy()
    chunk['집계구코드_str'] = chunk['집계구코드'].astype(str)
    filter_mask = chunk['집계구코드_str'].str[:7].isin(target_codes)
    chunk = chunk[filter_mask]
    
    if len(chunk) == 0:
        return pd.DataFrame()
    
    chunk['DATE'] = pd.to_datetime(chunk['기준일ID'].astype(str), format='%Y%m%d', errors='coerce')
    chunk['TIME'] = pd.to_numeric(chunk['시간대구분'], errors='coerce')
    wmap = {0:'월요일',1:'화요일',2:'수요일',3:'목요일',4:'금요일',5:'토요일',6:'일요일'}
    chunk['요일'] = chunk['DATE'].dt.dayofweek.map(wmap)
    chunk['주중_or_주말'] = np.where(chunk['DATE'].dt.dayofweek >= 5, '주말', '주중')
    
    def to_numeric_safe(col):
        return pd.to_numeric(chunk[col], errors='coerce').fillna(0)
    
    male_total = sum(to_numeric_safe(col) for col in male_cols)
    female_total = sum(to_numeric_safe(col) for col in female_cols)
    
    return pd.DataFrame({
        'DATE': chunk['DATE'].dt.strftime('%Y-%m-%d'),
        '요일': chunk['요일'],
        '주중_or_주말': chunk['주중_or_주말'],
        'TIME': chunk['TIME'],
        'CODE': chunk['집계구코드_str'],
        '남자': male_total,
        '여자': female_total
    }).dropna(subset=['DATE','TIME','CODE'])

st.warning("⚠️ 대용량 파일 처리용 - 파일당 수십MB 이상도 처리 가능")
st.info("📋 청크 크기: 10,000행씩 처리하여 메모리 사용량 최적화")

uploaded = st.file_uploader("대용량 CSV 파일 업로드", type='csv', accept_multiple_files=True)

if uploaded:
    st.subheader(f"업로드된 파일: {len(uploaded)}개")
    for f in uploaded:
        file_size_mb = len(f.read()) / (1024 * 1024)
        st.write(f"📄 {f.name} ({file_size_mb:.1f}MB)")
    
    if st.button("대용량 처리 시작"):
        file_bytes = {f.name: f.read() for f in uploaded}
        
        all_chunks = []
        total_files = len(file_bytes)
        progress = st.progress(0)
        
        for i, (fname, content) in enumerate(file_bytes.items(), start=1):
            st.write(f"🔄 {fname} 처리 중...")
            try:
                chunks = process_file_chunks(content, fname)
                all_chunks.extend(chunks)
                st.write(f"✅ {fname} 완료 - {len(chunks)}개 청크 처리됨")
            except Exception as e:
                st.error(f"❌ {fname} 실패: {e}")
            progress.progress(i / total_files)
        
        if all_chunks:
            st.write("🔄 최종 병합 중...")
            final_df = pd.concat(all_chunks, ignore_index=True).drop_duplicates()
            final_df = final_df.sort_values(['DATE','TIME','CODE']).reset_index(drop=True)
            
            st.success(f"🎉 완료! 총 {len(final_df):,}행 처리됨")
            
            bio = BytesIO()
            final_df.to_excel(bio, index=False, engine='openpyxl')
            bio.seek(0)
            fn = f"large_merged_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
            
            st.download_button(
                "📥 대용량 처리 결과 다운로드",
                data=bio.getvalue(),
                file_name=fn,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
