import pandas as pd
import os
from datetime import datetime

def classify_kospi_kosdaq_fixed():
    """
    실제 상장사만 필터링해서 코스피/코스닥 분류하는 프로그램
    """
    print("=" * 70)
    print("코스피/코스닥 실제 상장사 분류 프로그램 (수정 버전)")
    print("=" * 70)
    
    # 1. 종목코드.xlsx 파일 읽기
    print("\n1. 종목코드.xlsx 파일을 읽습니다...")
    try:
        df_stocks = pd.read_excel('종목코드.xlsx', sheet_name='종목코드', dtype={'COMP_CODE': str})
        print(f"   성공: 총 {len(df_stocks)}개 종목을 읽었습니다.")
    except Exception as e:
        print(f"   실패: {e}")
        return False
    
    # 2. 실제 상장사만 필터링
    print("\n2. 실제 상장사만 필터링합니다...")
    
    # 2-1. 숫자 종목코드만 (00000 제외)
    df_stocks['COMP_CODE_CLEAN'] = df_stocks['COMP_CODE'].fillna('').astype(str)
    numeric_mask = df_stocks['COMP_CODE_CLEAN'].str.isdigit()
    non_zero_mask = df_stocks['COMP_CODE_CLEAN'] != '00000'
    non_empty_mask = df_stocks['COMP_CODE_CLEAN'] != ''
    
    filtered_stocks = df_stocks[numeric_mask & non_zero_mask & non_empty_mask].copy()
    print(f"   - 유효한 숫자 종목코드: {len(filtered_stocks)}개")
    
    # 2-2. 비상장사 키워드 제외
    non_listed_keywords = [
        '사모', '투자조합', '펀드', '리츠', 'REITs', 'REIT', 
        '자산운용', '신탁', '기금', '조합', '합자', '합명', 
        '유한회사', '유한책임회사', '투자회사', '경영참여형사모',
        '블라인드펀드', '사모투자', '혼합자산', '부동산투자',
        '인프라투자', '사모부동산', '사모특별자산'
    ]
    
    # 키워드 필터링
    mask = True
    keyword_counts = {}
    for keyword in non_listed_keywords:
        keyword_mask = filtered_stocks['COMP_NAME'].str.contains(keyword, na=False)
        excluded_count = keyword_mask.sum()
        if excluded_count > 0:
            keyword_counts[keyword] = excluded_count
        mask = mask & (~keyword_mask)
    
    print(f"   - 제외된 비상장사 키워드:")
    for keyword, count in keyword_counts.items():
        print(f"     '{keyword}': {count}개")
    
    # 최종 필터링된 상장사
    listed_stocks = filtered_stocks[mask].copy()
    print(f"   - 최종 상장사 추정: {len(listed_stocks)}개")
    
    # 3. 종목코드로 코스피/코스닥 분류
    print("\n3. 코스피/코스닥으로 분류합니다...")
    
    def classify_exchange(stock_code):
        """실제 한국 증권시장 규칙으로 분류"""
        try:
            code_num = int(stock_code)
            
            # 한국 증권시장 종목코드 체계
            # 코스피: 000001~299999 (일반적으로)
            # 코스닥: 300000~ (특히 400000대가 많음)
            
            if 1 <= code_num <= 299999:
                return "KOSPI"
            elif 300000 <= code_num <= 399999:
                # 이 구간은 애매하지만 대부분 코스닥
                return "KOSDAQ"  
            elif 400000 <= code_num <= 999999:
                return "KOSDAQ"
            else:
                return "기타"
                
        except ValueError:
            return "기타"
    
    listed_stocks['거래소'] = listed_stocks['COMP_CODE_CLEAN'].apply(classify_exchange)
    
    # 분류 결과
    classification_counts = listed_stocks['거래소'].value_counts()
    print("   분류 결과:")
    for market, count in classification_counts.items():
        print(f"     - {market}: {count:,}개")
    
    # 4. 분류 검증을 위한 상세 분석
    print("\n4. 분류 결과 검증:")
    
    kospi_stocks = listed_stocks[listed_stocks['거래소'] == 'KOSPI'].copy()
    kosdaq_stocks = listed_stocks[listed_stocks['거래소'] == 'KOSDAQ'].copy()
    
    if not kospi_stocks.empty:
        kospi_codes = kospi_stocks['COMP_CODE_CLEAN'].astype(int)
        print(f"   - 코스피 종목코드 범위: {kospi_codes.min()} ~ {kospi_codes.max()}")
        
        # 구간별 분석
        print(f"     구간별 분포:")
        print(f"       1~99999: {len(kospi_codes[kospi_codes < 100000])}개")
        print(f"       100000~199999: {len(kospi_codes[(kospi_codes >= 100000) & (kospi_codes < 200000)])}개") 
        print(f"       200000~299999: {len(kospi_codes[(kospi_codes >= 200000) & (kospi_codes < 300000)])}개")
    
    if not kosdaq_stocks.empty:
        kosdaq_codes = kosdaq_stocks['COMP_CODE_CLEAN'].astype(int)
        print(f"   - 코스닥 종목코드 범위: {kosdaq_codes.min()} ~ {kosdaq_codes.max()}")
        
        # 구간별 분석
        print(f"     구간별 분포:")
        print(f"       300000~399999: {len(kosdaq_codes[(kosdaq_codes >= 300000) & (kosdaq_codes < 400000)])}개")
        print(f"       400000~499999: {len(kosdaq_codes[(kosdaq_codes >= 400000) & (kosdaq_codes < 500000)])}개")
        print(f"       500000~: {len(kosdaq_codes[kosdaq_codes >= 500000])}개")
    
    # 5. 파일 저장
    print("\n5. 분류 결과를 저장합니다...")
    
    try:
        os.makedirs('doc', exist_ok=True)
        
        # 전체 실제 상장사 저장
        listed_stocks.to_excel('doc/실제상장사_분류결과.xlsx', index=False, sheet_name='전체상장사')
        print(f"   성공: doc/실제상장사_분류결과.xlsx 저장 완료 ({len(listed_stocks)}개)")
        
        # 코스피만 저장
        if not kospi_stocks.empty:
            kospi_stocks.to_excel('doc/코스피_실제상장사.xlsx', index=False, sheet_name='코스피')
            print(f"   성공: doc/코스피_실제상장사.xlsx 저장 완료 ({len(kospi_stocks)}개)")
        
        # 코스닥만 저장  
        if not kosdaq_stocks.empty:
            kosdaq_stocks.to_excel('doc/코스닥_실제상장사.xlsx', index=False, sheet_name='코스닥')
            print(f"   성공: doc/코스닥_실제상장사.xlsx 저장 완료 ({len(kosdaq_stocks)}개)")
        
    except Exception as e:
        print(f"   저장 실패: {e}")
        return False
    
    # 6. 대표 종목 미리보기
    print(f"\n6. 코스피 대표 종목 (상위 20개):")
    if not kospi_stocks.empty:
        kospi_preview = kospi_stocks.head(20)
        for _, row in kospi_preview.iterrows():
            print(f"   {row['COMP_CODE_CLEAN']}: {row['COMP_NAME']}")
    
    print(f"\n7. 코스닥 대표 종목 (상위 10개):")
    if not kosdaq_stocks.empty:
        kosdaq_preview = kosdaq_stocks.head(10)
        for _, row in kosdaq_preview.iterrows():
            print(f"   {row['COMP_CODE_CLEAN']}: {row['COMP_NAME']}")
    
    return True

def main():
    """메인 함수"""
    start_time = datetime.now()
    print(f"시작 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    success = classify_kospi_kosdaq_fixed()
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 70)
    if success:
        print("[SUCCESS] 실제 상장사 분류가 완료되었습니다!")
        print("이제 실제 코스피/코스닥 상장사만으로 재무분석을 진행할 수 있습니다.")
    else:
        print("[ERROR] 분류 작업이 실패했습니다.")
    
    print(f"완료 시간: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"소요 시간: {duration}")
    print("=" * 70)

if __name__ == "__main__":
    main()