import streamlit as st
from io import BytesIO
from datetime import datetime
import pandas as pd
from utils import merge_files

st.set_page_config(page_title="생활인구 데이터 취합", layout="wide")

st.title("생활인구 CSV 파일 병합 및 다운로드")

uploaded = st.file_uploader(
    "CSV 파일을 업로드하세요 (여러 개 선택 가능)", 
    type=['csv'], accept_multiple_files=True
)

if uploaded:
    try:
        # 파일 내용을 dict 형태로 변환
        files_dict = {f.name: f.read() for f in uploaded}
        with st.spinner("파일 처리 중... 잠시만 기다려주세요"):
            merged_df = merge_files(files_dict)
        st.success(f"✅ 처리 완료: 총 {len(merged_df):,}행")

        # 미리보기
        st.dataframe(merged_df.head(10))

        # 다운로드 버튼
        towrite = BytesIO()
        merged_df.to_excel(towrite, index=False, engine='openpyxl')
        towrite.seek(0)
        fn = f"merged_population_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
        st.download_button(
            label="엑셀 파일 다운로드",
            data=towrite,
            file_name=fn,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as err:
        st.error(f"❌ 오류 발생: {err}")
