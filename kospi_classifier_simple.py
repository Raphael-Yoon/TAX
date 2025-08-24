import pandas as pd
import os
from datetime import datetime

def classify_kospi_kosdaq_simple():
    """
    종목코드.xlsx 파일에서 코스피/코스닥 상장사를 분류하는 프로그램
    종목코드 패턴만으로 분류 (빠른 처리)
    """
    print("=" * 70)
    print("코스피/코스닥 상장사 분류 프로그램 (간소화 버전)")
    print("=" * 70)
    
    # 1. 종목코드.xlsx 파일 읽기
    print("\n1. 종목코드.xlsx 파일을 읽습니다...")
    try:
        df_stocks = pd.read_excel('종목코드.xlsx', sheet_name='종목코드', dtype={'COMP_CODE': str})
        df_stocks['COMP_CODE'] = df_stocks['COMP_CODE'].str.zfill(6)
        print(f"   성공: 총 {len(df_stocks)}개 종목을 읽었습니다.")
    except Exception as e:
        print(f"   실패: {e}")
        return False
    
    # 2. 종목코드 패턴으로 분류
    print("\n2. 종목코드 패턴으로 거래소를 분류합니다...")
    
    def classify_by_code_pattern(stock_code):
        """종목코드 패턴으로 거래소 분류"""
        if pd.isna(stock_code) or stock_code is None:
            return "기타"
            
        code = str(stock_code).strip()
        
        # 숫자가 아닌 경우
        if not code.isdigit():
            return "기타"
        
        try:
            code_num = int(code)
            
            # 한국 주식시장 일반적인 종목코드 규칙
            # 000001~399999: 코스피 (유가증권시장)  
            # 400000~999999: 코스닥
            if 1 <= code_num <= 399999:
                return "KOSPI"
            elif 400000 <= code_num <= 999999:
                return "KOSDAQ"
            else:
                return "기타"
                
        except ValueError:
            return "기타"
    
    # 분류 적용
    df_stocks['거래소'] = df_stocks['COMP_CODE'].apply(classify_by_code_pattern)
    
    # 분류 결과 출력
    classification_counts = df_stocks['거래소'].value_counts()
    print("   분류 결과:")
    for market, count in classification_counts.items():
        print(f"     - {market}: {count:,}개")
    
    # 3. 종목코드별 분포 분석
    print("\n3. 종목코드 분포 분석:")
    
    # 코스피 종목코드 범위 분석
    kospi_stocks = df_stocks[df_stocks['거래소'] == 'KOSPI'].copy()
    if not kospi_stocks.empty:
        kospi_codes = kospi_stocks['COMP_CODE'].astype(int)
        print(f"   - 코스피: {kospi_codes.min()} ~ {kospi_codes.max()}")
        print(f"     구간별 분포:")
        print(f"       1~99999: {len(kospi_codes[kospi_codes < 100000])}개")
        print(f"       100000~199999: {len(kospi_codes[(kospi_codes >= 100000) & (kospi_codes < 200000)])}개")
        print(f"       200000~299999: {len(kospi_codes[(kospi_codes >= 200000) & (kospi_codes < 300000)])}개")
        print(f"       300000~399999: {len(kospi_codes[(kospi_codes >= 300000) & (kospi_codes < 400000)])}개")
    
    # 코스닥 종목코드 범위 분석
    kosdaq_stocks = df_stocks[df_stocks['거래소'] == 'KOSDAQ'].copy()
    if not kosdaq_stocks.empty:
        kosdaq_codes = kosdaq_stocks['COMP_CODE'].astype(int)
        print(f"   - 코스닥: {kosdaq_codes.min()} ~ {kosdaq_codes.max()}")
        print(f"     구간별 분포:")
        print(f"       400000~499999: {len(kosdaq_codes[(kosdaq_codes >= 400000) & (kosdaq_codes < 500000)])}개")
        print(f"       500000~599999: {len(kosdaq_codes[(kosdaq_codes >= 500000) & (kosdaq_codes < 600000)])}개")
        print(f"       600000~699999: {len(kosdaq_codes[(kosdaq_codes >= 600000) & (kosdaq_codes < 700000)])}개")
        print(f"       700000~999999: {len(kosdaq_codes[kosdaq_codes >= 700000])}개")
    
    # 4. 분류된 결과를 엑셀 파일로 저장
    print("\n4. 분류 결과를 저장합니다...")
    
    try:
        # 디렉토리 생성
        os.makedirs('doc', exist_ok=True)
        
        # 전체 결과 저장
        df_stocks.to_excel('doc/종목분류결과_간소화.xlsx', index=False, sheet_name='전체종목')
        print(f"   성공: doc/종목분류결과_간소화.xlsx 저장 완료")
        
        # 코스피만 따로 저장
        if not kospi_stocks.empty:
            kospi_stocks.to_excel('doc/코스피종목_간소화.xlsx', index=False, sheet_name='코스피')
            print(f"   성공: doc/코스피종목_간소화.xlsx 저장 완료 ({len(kospi_stocks):,}개)")
        
        # 코스닥만 따로 저장
        if not kosdaq_stocks.empty:
            kosdaq_stocks.to_excel('doc/코스닥종목_간소화.xlsx', index=False, sheet_name='코스닥')
            print(f"   성공: doc/코스닥종목_간소화.xlsx 저장 완료 ({len(kosdaq_stocks):,}개)")
        
    except Exception as e:
        print(f"   저장 실패: {e}")
        return False
    
    # 5. 코스피 대표 종목 미리보기
    print(f"\n5. 코스피 대표 종목 미리보기 (상위 20개):")
    if not kospi_stocks.empty:
        kospi_preview = kospi_stocks.head(20)
        for idx, row in kospi_preview.iterrows():
            print(f"   {row['COMP_CODE']}: {row['COMP_NAME']}")
    else:
        print("   코스피 종목이 없습니다.")
    
    # 6. 코스닥 대표 종목 미리보기
    print(f"\n6. 코스닥 대표 종목 미리보기 (상위 10개):")
    if not kosdaq_stocks.empty:
        kosdaq_preview = kosdaq_stocks.head(10)
        for idx, row in kosdaq_preview.iterrows():
            print(f"   {row['COMP_CODE']}: {row['COMP_NAME']}")
    else:
        print("   코스닥 종목이 없습니다.")
    
    return True

def main():
    """메인 함수"""
    start_time = datetime.now()
    print(f"시작 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    success = classify_kospi_kosdaq_simple()
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 70)
    if success:
        print("[SUCCESS] 코스피/코스닥 분류가 완료되었습니다!")
    else:
        print("[ERROR] 분류 작업이 실패했습니다.")
    
    print(f"완료 시간: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"소요 시간: {duration}")
    print("=" * 70)

if __name__ == "__main__":
    main()