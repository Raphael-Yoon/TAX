import pandas as pd

def analyze_stock_data():
    """종목코드 데이터를 분석해서 문제점 파악"""
    
    print("=" * 60)
    print("종목코드 데이터 분석")
    print("=" * 60)
    
    # 1. 원본 데이터 읽기
    try:
        df = pd.read_excel('종목코드.xlsx', sheet_name='종목코드', dtype={'COMP_CODE': str})
        print(f"\n1. 전체 데이터: {len(df)}개")
        print("컬럼:", list(df.columns))
    except Exception as e:
        print(f"파일 읽기 실패: {e}")
        return
    
    # 2. 데이터 타입과 샘플 확인
    print(f"\n2. 데이터 샘플 (상위 20개):")
    for i, row in df.head(20).iterrows():
        print(f"   {row['COMP_CODE']}: {row['COMP_NAME']}")
    
    # 3. COMP_CODE 패턴 분석
    print(f"\n3. COMP_CODE 패턴 분석:")
    
    # 숫자만 있는 코드와 문자가 포함된 코드 분리
    df['COMP_CODE_CLEAN'] = df['COMP_CODE'].fillna('').astype(str)
    numeric_codes = df[df['COMP_CODE_CLEAN'].str.isdigit()].copy()
    non_numeric_codes = df[~df['COMP_CODE_CLEAN'].str.isdigit()].copy()
    
    print(f"   - 숫자만 있는 코드: {len(numeric_codes)}개")
    print(f"   - 문자 포함 코드: {len(non_numeric_codes)}개")
    
    if len(non_numeric_codes) > 0:
        print(f"   - 문자 포함 코드 예시:")
        for i, row in non_numeric_codes.head(10).iterrows():
            print(f"     {row['COMP_CODE']}: {row['COMP_NAME']}")
    
    # 4. 숫자 코드 범위 분석
    if len(numeric_codes) > 0:
        print(f"\n4. 숫자 코드 범위 분석:")
        numeric_codes['CODE_INT'] = numeric_codes['COMP_CODE_CLEAN'].astype(int)
        
        print(f"   - 최소값: {numeric_codes['CODE_INT'].min()}")
        print(f"   - 최대값: {numeric_codes['CODE_INT'].max()}")
        
        # 구간별 분석
        ranges = [
            (0, 99999, "0~99999"),
            (100000, 199999, "100000~199999"), 
            (200000, 299999, "200000~299999"),
            (300000, 399999, "300000~399999"),
            (400000, 499999, "400000~499999"),
            (500000, 599999, "500000~599999"),
            (600000, 999999, "600000~999999")
        ]
        
        for min_val, max_val, label in ranges:
            count = len(numeric_codes[(numeric_codes['CODE_INT'] >= min_val) & 
                                    (numeric_codes['CODE_INT'] <= max_val)])
            if count > 0:
                print(f"   - {label}: {count}개")
                
                # 각 구간의 샘플 5개씩
                sample = numeric_codes[(numeric_codes['CODE_INT'] >= min_val) & 
                                     (numeric_codes['CODE_INT'] <= max_val)].head(5)
                for _, row in sample.iterrows():
                    print(f"     {row['COMP_CODE']}: {row['COMP_NAME']}")
                print()
    
    # 5. 실제 상장사 여부 확인을 위한 키워드 분석
    print(f"\n5. 회사명 키워드 분석:")
    
    # 비상장 관련 키워드들
    non_listed_keywords = ['사모', '투자조합', '펀드', '리츠', 'REITs', '증권', '자산운용', 
                          '투자', '신탁', '기금', '조합', '합자', '합명', '유한회사']
    
    keyword_counts = {}
    for keyword in non_listed_keywords:
        count = len(df[df['COMP_NAME'].str.contains(keyword, na=False)])
        if count > 0:
            keyword_counts[keyword] = count
    
    print("   비상장 관련 키워드가 포함된 회사 수:")
    for keyword, count in keyword_counts.items():
        print(f"   - '{keyword}' 포함: {count}개")
    
    # 실제 상장사로 보이는 회사들만 추출 (키워드 필터링)
    print(f"\n6. 실제 상장사 추정:")
    
    # 비상장 키워드가 포함되지 않은 회사들만
    mask = True
    for keyword in non_listed_keywords:
        mask = mask & (~df['COMP_NAME'].str.contains(keyword, na=False))
    
    likely_listed = df[mask & df['COMP_CODE_CLEAN'].str.isdigit()].copy()
    print(f"   키워드 필터링 후 상장사 추정: {len(likely_listed)}개")
    
    if len(likely_listed) > 0:
        likely_listed['CODE_INT'] = likely_listed['COMP_CODE_CLEAN'].astype(int)
        
        # 코스피/코스닥 추정 범위로 분류
        kospi_estimated = likely_listed[likely_listed['CODE_INT'] < 400000]
        kosdaq_estimated = likely_listed[likely_listed['CODE_INT'] >= 400000]
        
        print(f"   - 코스피 추정 (1~399999): {len(kospi_estimated)}개")
        print(f"   - 코스닥 추정 (400000~): {len(kosdaq_estimated)}개")
        
        print(f"\n   코스피 추정 종목 샘플:")
        for _, row in kospi_estimated.head(10).iterrows():
            print(f"   {row['COMP_CODE']}: {row['COMP_NAME']}")

if __name__ == "__main__":
    analyze_stock_data()