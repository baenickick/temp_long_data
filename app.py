# app.py
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import io

st.set_page_config(page_title="생활인구 CSV 병합 (지역 선택)", layout="wide")
st.title("생활인구 CSV 병합 웹앱 (지역 선택 필터)")

# 지역 매칭 데이터
region_data = """통계청행정동코드	시도명	시군구명	행정동명
1101053	서울	종로구	사직동
1101054	서울	종로구	삼청동
1101055	서울	종로구	부암동
1101056	서울	종로구	평창동
1101057	서울	종로구	무악동
1101058	서울	종로구	교남동
1101060	서울	종로구	가회동
1101061	서울	종로구	종로1.2.3.4가동
1101063	서울	종로구	종로5.6가동
1101064	서울	종로구	이화동
1101067	서울	종로구	창신1동
1101068	서울	종로구	창신2동
1101069	서울	종로구	창신3동
1101070	서울	종로구	숭인1동
1101071	서울	종로구	숭인2동
1101072	서울	종로구	청운효자동
1101073	서울	종로구	혜화동
1102052	서울	중구	소공동
1102054	서울	중구	회현동
1102055	서울	중구	명동
1102057	서울	중구	필동
1102058	서울	중구	장충동
1102059	서울	중구	광희동
1102060	서울	중구	을지로동
1102065	서울	중구	신당5동
1102067	서울	중구	황학동
1102068	서울	중구	중림동
1102069	서울	중구	신당동
1102070	서울	중구	다산동
1102071	서울	중구	약수동
1102072	서울	중구	청구동
1102073	서울	중구	동화동
1103051	서울	용산구	후암동
1103052	서울	용산구	용산2가동
1103053	서울	용산구	남영동
1103057	서울	용산구	원효로2동
1103058	서울	용산구	효창동
1103059	서울	용산구	용문동
1103063	서울	용산구	이촌1동
1103064	서울	용산구	이촌2동
1103065	서울	용산구	이태원1동
1103066	서울	용산구	이태원2동
1103069	서울	용산구	서빙고동
1103070	서울	용산구	보광동
1103071	서울	용산구	청파동
1103072	서울	용산구	원효로1동
1103073	서울	용산구	한강로동
1103074	서울	용산구	한남동
1104052	서울	성동구	왕십리2동
1104054	서울	성동구	마장동
1104055	서울	성동구	사근동
1104056	서울	성동구	행당1동
1104057	서울	성동구	행당2동
1104058	서울	성동구	응봉동
1104059	서울	성동구	금고1가동
1104062	서울	성동구	금고4가동
1104065	서울	성동구	성수1가1동
1104066	서울	성동구	성수1가2동
1104067	서울	성동구	성수2가1동
1104068	서울	성동구	성수2가3동
1104069	서울	성동구	송정동
1104070	서울	성동구	용답동
1104071	서울	성동구	왕십리도선동
1104072	서울	성동구	금고2.3가동
1104073	서울	성동구	옥수동"""

def load_region_mapping():
    """지역 매핑 데이터를 로드하여 DataFrame으로 변환"""
    lines = region_data.strip().split('\n')[1:]  # 헤더 제외
    data = []
    for line in lines:
        parts = line.split('\t')
        if len(parts) >= 4:
            data.append({
                '코드': parts[0],
                '시도': parts[1],
                '시군구': parts[2], 
                '행정동': parts[3],
                '코드7자리': parts[0][:7]
            })
    return pd.DataFrame(data)

def detect_delimiter(sample_bytes):
    s = sample_bytes[:2048].decode('utf-8', errors='ignore')
    return '\t' if s.count('\t') > s.count(',') else ','

def process_file(content_bytes, filename, selected_codes):
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
    
    # 선택된 지역의 7자리 코드로 필터링
    mask = df['집계구코드_str'].str[:7].isin(selected_codes)
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

# 지역 매핑 데이터 로드
region_df = load_region_mapping()

# 상단에 지역 선택 UI (가로 배치)
st.header("📍 지역 선택")

# 시군구별 그룹화
districts = region_df.groupby('시군구')['행정동'].apply(list).to_dict()
selected_regions = []

for district, dongs in districts.items():
    st.subheader(f"🏢 {district}")
    
    # 행정동을 4개씩 나누어 컬럼으로 배치
    cols = st.columns(4)
    for idx, dong in enumerate(dongs):
        col_idx = idx % 4
        with cols[col_idx]:
            key = f"{district}_{dong}"
            if st.checkbox(f"{dong}", key=key):
                # 해당 행정동의 7자리 코드 찾기
                code_7 = region_df[region_df['행정동'] == dong]['코드7자리'].iloc[0]
                selected_regions.append({
                    'district': district,
                    'dong': dong,
                    'code': code_7
                })

st.markdown("---")

# 선택된 지역 표시
if selected_regions:
    st.success(f"✅ 선택된 지역: {len(selected_regions)}개")
    selected_codes = [r['code'] for r in selected_regions]
    
    # 선택된 지역을 3컬럼으로 표시
    selected_cols = st.columns(3)
    for idx, region in enumerate(selected_regions):
        col_idx = idx % 3
        with selected_cols[col_idx]:
            st.write(f"• **{region['district']}** {region['dong']}")
    
    st.markdown("---")
    
    # 파일 업로드
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
        
        if st.button("🚀 선택된 지역으로 필터링 실행"):
            progress = st.progress(0)
            dfs, errors = [], []
            total = len(files_dict)
            
            for i, (fname, content) in enumerate(files_dict.items(), start=1):
                try:
                    dfp = process_file(content, fname, selected_codes)
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
                st.success(f"🎉 병합 완료: 총 {len(merged):,}행")
                
                # 포함된 지역 코드 표시
                unique_codes = merged['CODE'].str[:7].unique()
                matched_regions = []
                for code in unique_codes:
                    matches = region_df[region_df['코드7자리'] == code]
                    if not matches.empty:
                        matched_regions.append(f"{matches.iloc[0]['시군구']} {matches.iloc[0]['행정동']}")
                
                if matched_regions:
                    st.info(f"📊 최종 포함된 지역: {', '.join(matched_regions)}")
                
                bio = BytesIO()
                merged.to_excel(bio, index=False, engine='openpyxl')
                bio.seek(0)
                data = bio.getvalue()
                fn = f"selected_regions_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
                st.download_button(
                    "📥 선택된 지역 엑셀 다운로드",
                    data=data,
                    file_name=fn,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            if errors:
                st.error("❌ 오류 발생:")
                for e in errors:
                    st.write("-", e)
else:
    st.warning("⚠️ 분석하고 싶은 지역을 위에서 선택해주세요.")
    st.info("💡 여러 지역을 선택할 수 있습니다. 체크박스를 클릭하여 원하는 지역들을 선택하세요.")
