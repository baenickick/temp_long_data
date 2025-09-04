import pandas as pd
import numpy as np
import io
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

def detect_delimiter_fast(sample_bytes):
    sample = sample_bytes[:2048].decode('utf-8', errors='ignore')
    return '\t' if sample.count('\t') > sample.count(',') else ','

def process_single_file(content, filename):
    # 1) 구분자 감지
    delimiter = detect_delimiter_fast(content)
    # 2) 인코딩 감지 및 읽기
    df = None
    for enc in ['utf-8','cp949','utf-8-sig']:
        try:
            df = pd.read_csv(io.BytesIO(content),
                             encoding=enc,
                             delimiter=delimiter,
                             low_memory=False,
                             dtype=str)
            break
        except:
            continue
    if df is None:
        raise ValueError(f"{filename}: 인코딩 오류")
    # 3) 컬럼 정리
    df.columns = df.columns.str.strip().str.replace('"','').str.replace('?','')
    req = ['기준일ID','시간대구분','집계구코드','총생활인구수',
           '중국인체류인구수','중국외외국인체류인구수']
    if not all(c in df.columns for c in req):
        raise ValueError(f"{filename}: 필수 컬럼 누락")
    df = df[req].copy()
    # 4) 날짜/시간/코드/요일/주중구분 처리
    df['DATE'] = pd.to_datetime(df['기준일ID'].astype(str),
                                format='%Y%m%d', errors='coerce')
    df['TIME']  = pd.to_numeric(df['시간대구분'], errors='coerce')
    weekday = {0:'월요일',1:'화요일',2:'수요일',3:'목요일',
               4:'금요일',5:'토요일',6:'일요일'}
    df['요일'] = df['DATE'].dt.dayofweek.map(weekday)
    df['주중_or_주말'] = np.where(df['DATE'].dt.dayofweek>=5,'주말','주중')
    df['CODE'] = (pd.to_numeric(df['집계구코드'],errors='coerce')
                  .astype('Int64').astype(str))
    # 5) 최종 칼럼 선택
    return pd.DataFrame({
        'DATE': df['DATE'].dt.strftime('%Y-%m-%d'),
        '요일': df['요일'],
        '주중_or_주말': df['주중_or_주말'],
        'TIME': df['TIME'],
        'CODE': df['CODE'],
        'ALL':  pd.to_numeric(df['총생활인구수'],errors='coerce'),
        'CHN':  pd.to_numeric(df['중국인체류인구수'],errors='coerce'),
        'EXP_CHN': pd.to_numeric(df['중국외외국인체류인구수'],
                                errors='coerce')
    }).dropna(subset=['DATE','TIME','CODE'])

def merge_files(uploaded_files):
    dfs, errors = [], []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(process_single_file, content, fn): fn
                   for fn, content in uploaded_files.items()}
        for fut in as_completed(futures):
            fn = futures[fut]
            try:
                dfs.append(fut.result())
            except Exception as e:
                errors.append(str(e))
    if not dfs:
        raise ValueError("처리 가능한 데이터 없음:\n" + "\n".join(errors))
    merged = pd.concat(dfs, ignore_index=True).drop_duplicates()
    merged['DATE'] = pd.Categorical(merged['DATE'])
    merged = merged.sort_values(['DATE','TIME','CODE']).reset_index(drop=True)
    return merged
