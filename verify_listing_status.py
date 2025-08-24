import pandas as pd
import yfinance as yf
import time

def verify_actual_listing():
    """실제 상장 여부를 샘플링으로 검증"""
    
    print("=" * 60)
    print("실제 상장 여부 검증 프로그램")
    print("=" * 60)
    
    # 분류된 코스피 데이터 읽기
    try:
        kospi_df = pd.read_excel('doc/코스피_실제상장사.xlsx', dtype={'COMP_CODE_CLEAN': str})
        print(f"코스피 분류된 종목 수: {len(kospi_df)}개")
    except Exception as e:
        print(f"파일 읽기 실패: {e}")
        return
    
    # 코스닥 데이터도 읽기
    try:
        kosdaq_df = pd.read_excel('doc/코스닥_실제상장사.xlsx', dtype={'COMP_CODE_CLEAN': str})
        print(f"코스닥 분류된 종목 수: {len(kosdaq_df)}개")
    except Exception as e:
        print(f"코스닥 파일 읽기 실패: {e}")
        kosdaq_df = pd.DataFrame()
    
    # 샘플링으로 실제 상장 여부 확인
    print(f"\n실제 상장 여부 샘플 검증 (각각 20개씩):")
    
    def check_real_listing(df, market_name, suffix):
        """실제 거래소에서 확인"""
        print(f"\n=== {market_name} 샘플 검증 ===")
        
        sample = df.head(20).copy()
        actual_listed = 0
        not_listed = 0
        
        for idx, row in sample.iterrows():
            stock_code = row['COMP_CODE_CLEAN']
            company_name = row['COMP_NAME']
            
            try:
                ticker = f"{stock_code}.{suffix}"
                stock = yf.Ticker(ticker)
                info = stock.info
                
                # 기본적인 주식 정보가 있는지 확인
                if info and 'symbol' in info and info.get('regularMarketPrice'):
                    print(f"✅ {stock_code}: {company_name} - 실제 상장")
                    actual_listed += 1
                else:
                    print(f"❌ {stock_code}: {company_name} - 상장 확인 불가")
                    not_listed += 1
                    
            except Exception as e:
                print(f"⚠️  {stock_code}: {company_name} - 조회 실패")
                not_listed += 1
            
            time.sleep(0.2)  # API 제한
        
        print(f"\n{market_name} 검증 결과:")
        print(f"  실제 상장: {actual_listed}개")
        print(f"  상장 확인 불가: {not_listed}개")
        print(f"  상장 비율: {actual_listed/20*100:.1f}%")
        
        return actual_listed, not_listed
    
    # 코스피 검증
    kospi_listed, kospi_not = check_real_listing(kospi_df, "코스피", "KS")
    
    # 코스닥 검증
    if not kosdaq_df.empty:
        kosdaq_listed, kosdaq_not = check_real_listing(kosdaq_df, "코스닥", "KQ")
    else:
        kosdaq_listed, kosdaq_not = 0, 0
    
    # 전체 결과 분석
    print(f"\n" + "=" * 60)
    print("전체 검증 결과 요약:")
    print(f"코스피: 실제상장 {kospi_listed}/20개 ({kospi_listed/20*100:.1f}%)")
    if kosdaq_df is not None and not kosdaq_df.empty:
        print(f"코스닥: 실제상장 {kosdaq_listed}/20개 ({kosdaq_listed/20*100:.1f}%)")
    
    # 추정치 계산
    if kospi_listed > 0:
        estimated_kospi = int(len(kospi_df) * (kospi_listed / 20))
        print(f"\n추정 실제 코스피 상장사: 약 {estimated_kospi:,}개")
    
    if kosdaq_listed > 0:
        estimated_kosdaq = int(len(kosdaq_df) * (kosdaq_listed / 20))  
        print(f"추정 실제 코스닥 상장사: 약 {estimated_kosdaq:,}개")
    
    print(f"\n참고: 실제 한국 증권시장")
    print(f"- 코스피 상장사: 약 800~900개")
    print(f"- 코스닥 상장사: 약 1,500~1,700개")
    print("=" * 60)

def analyze_company_names():
    """회사명 패턴 분석으로 실제 상장사 추가 필터링"""
    
    print("\n회사명 패턴 분석:")
    
    try:
        kospi_df = pd.read_excel('doc/코스피_실제상장사.xlsx')
        
        # 추가 필터링할 키워드들
        additional_filters = [
            '호', '1호', '2호', '3호', '4호', '5호', '6호', '7호', '8호', '9호',
            '제1호', '제2호', '제3호', '제4호', '제5호',
            '스팩', 'SPAC', '블랭크체크',
            '상장지수펀드', 'ETF', 'ETN',
            '리츠', 'REITs', 'REIT',
            '투자회사', '투자법인', '투자신탁',
            '경영참여형', '사모투자', '벤처투자',
            '인수목적회사', '특수목적회사'
        ]
        
        print(f"현재 코스피 분류된 종목 수: {len(kospi_df)}개")
        
        # 추가 필터링
        final_mask = True
        removed_counts = {}
        
        for keyword in additional_filters:
            keyword_mask = kospi_df['COMP_NAME'].str.contains(keyword, na=False)
            removed_count = keyword_mask.sum()
            if removed_count > 0:
                removed_counts[keyword] = removed_count
                print(f"  '{keyword}' 포함: {removed_count}개 제외 예정")
            final_mask = final_mask & (~keyword_mask)
        
        final_kospi = kospi_df[final_mask]
        print(f"\n추가 필터링 후 코스피: {len(final_kospi)}개")
        
        # 샘플 확인
        print(f"\n남은 코스피 종목 샘플 (상위 20개):")
        for _, row in final_kospi.head(20).iterrows():
            print(f"  {row['COMP_CODE_CLEAN']}: {row['COMP_NAME']}")
            
        return len(final_kospi)
        
    except Exception as e:
        print(f"분석 실패: {e}")
        return None

if __name__ == "__main__":
    verify_actual_listing()
    final_count = analyze_company_names()
    
    if final_count:
        print(f"\n최종 결론:")
        print(f"엄격한 필터링 후 추정 코스피 상장사: {final_count}개")
        if 700 <= final_count <= 1000:
            print("✅ 실제 코스피 상장사 수와 비슷한 수준입니다.")
        else:
            print("⚠️ 여전히 실제와 차이가 있을 수 있습니다.")