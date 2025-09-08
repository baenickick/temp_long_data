# app.py
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import io

st.set_page_config(page_title="국내인구 CSV 병합 (필터링)", layout="wide")
st.title("국내인구 CSV 병합 웹앱 (LOCAL_PEOPLE 파일 전용)")

def detect_delimiter(sample_bytes):
    s = sample_bytes[:2048].decode('utf-8', errors='ignore')
    return '\t' if s.count('\t') > s.count(',') else ','

def process_file(content_bytes, filename):
    delim = detect_delimiter(content_bytes)
    df = None
    # 인코딩 목록 확장: utf-8, utf-8-sig, cp949, euc-kr
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
        except Exception:
            continue
    if df is None:
        raise ValueError(f"{filename}: 인코딩 실패")
    
    # 컬럼 정리
    df.columns = df.columns.str.strip().str.replace('"','').str.replace('?','')
    
    # 기본 및 연령대별 컬럼 목록
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
        missing = [c for c in required if c not in df.columns]
        raise ValueError(f"{filename}: 필수 컬럼 누락 - {missing}")
    
    df = df[required].copy()
    
    # CODE 앞 7자리 필터링
    target_codes = ['1104065','1104066','1104067','1104068']
    df['집계구코드_str'] = df['집계구코드'].astype(str)
    mask = df['집계구코드_str'].str[:7].isin(target_codes)
    df = df[mask].copy()
    
    if df.empty:
        return pd.DataFrame()
    
    # 공통 변환
    df['DATE'] = pd.to_datetime(df['기준일ID'].astype(str), format='%Y%m%d', errors='coerce')
    df['TIME'] = pd.to_numeric(df['시간대구분'], errors='coerce')
    wmap = {0:'월요일',1:'화요일',2:'수요일',3:'목요일',4:'금요일',5:'토요일',6:'일요일'}
    df['요일'] = df['DATE'].dt.dayofweek.map(wmap)
    df['주중_or_주말'] = np.where(df['DATE'].dt.dayofweek>=5,'주말','주중')
    df['CODE'] = df['집계구코드_str']
    
    # 안전한 숫자 변환
    def to_num(col): return pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # 성별 총합
    male_total = sum(to_num(c) for c in male_cols)
    female_total = sum(to_num(c) for c in female_cols)
    
    # 연령대별 합산
    result = pd.DataFrame({
        'DATE': df['DATE'].dt.strftime('%Y-%m-%d'),
        '요일': df['요일'],
        '주중_or_주말': df['주중_or_주말'],
        'TIME': df['TIME'],
        'CODE': df['CODE'],
        '남자': male_total,
        '여자': female_total,
        '남자 0-9': to_num(male_cols[0]),
        '남자 10-19': to_num(male_cols[1]) + to_num(male_cols[2]),
        '남자 20-29': to_num(male_cols[3]) + to_num(male_cols[4]),
        '남자 30-39': to_num(male_cols[5]) + to_num(male_cols[6]),
        '남자 40-49': to_num(male_cols[7]) + to_num(male_cols[8]),
        '남자 50-59': to_num(male_cols[9]) + to_num(male_cols[10]),
        '남자 60-69': to_num(male_cols[11]) + to_num(male_cols[12]),
        '남자 70+': to_num(male_cols[13]),
        '여자 0-9': to_num(female_cols[0]),
        '여자 10-19': to_num(female_cols[1]) + to_num(female_cols[2]),
        '여자 20-29': to_num(female_cols[3]) + to_num(female_cols[4]),
        '여자 30-39': to_num(female_cols[5]) + to_num(female_cols[6]),
        '여자 40-49': to_num(female_cols[7]) + to_num(female_cols[8]),
        '여자 50-59': to_num(female_cols[9]) + to_num(female_cols[10]),
        '여자 60-69': to_num(female_cols[11]) + to_num(female_cols[12]),
        '여자 70+': to_num(female_cols[13])
    }).dropna(subset=['DATE','TIME','CODE'])
    
    return result

st.info("📋 LOCAL_PEOPLE 파일 전용 처리 (인코딩: utf-8, utf-8-sig, cp949, euc-kr)")
st.info("📋 CODE 앞 7자리 필터링: 1104065~1104068")

uploaded = st.file_uploader("LOCAL_PEOPLE CSV 업로드", type='csv', accept_multiple_files=True)

if uploaded:
    files_dict = {f.name: f.read() for f in uploaded if f.name.startswith('LOCAL_PEOPLE')}
    if not files_dict:
        st.error("LOCAL_PEOPLE로 시작하는 파일이 없습니다.")
    else:
        st.success(f"감지된 파일: {len(files_dict)}개")
        # 미리보기
        st.subheader("첫 파일 5행 미리보기")
        name0 = list(files_dict)[0]
        sample = files_dict[name0]
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
            dfs, errs = [], []
            total = len(files_dict)
            for i,(nm,cont) in enumerate(files_dict.items(),start=1):
                try:
                    dfp = process_file(cont, nm)
                    if not dfp.empty:
                        dfs.append(dfp)
                        st.write(f"✓ {nm}: {len(dfp):,}행")
                    else:
                        st.write(f"○ {nm}: 0행")
                except Exception as e:
                    errs.append(str(e))
                    st.write(f"✗ {nm}: {e}")
                progress.progress(i/total)
            
            if dfs:
                merged = pd.concat(dfs,ignore_index=True).drop_duplicates()
                merged['DATE'] = pd.Categorical(merged['DATE'])
                merged = merged.sort_values(['DATE','TIME','CODE']).reset_index(drop=True)
                st.success(f"✅ 병합 완료: 총 {len(merged):,}행")
                bio = BytesIO()
                merged.to_excel(bio, index=False, engine='openpyxl')
                bio.seek(0)
                data = bio.getvalue()
                fn = f"local_merged_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
                st.download_button("엑셀 다운로드", data=data, file_name=fn,
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            if errs:
                st.error("오류:")
                for e in errs: st.write("-",e)
