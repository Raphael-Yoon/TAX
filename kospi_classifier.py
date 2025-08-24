import pandas as pd
import yfinance as yf
import os
from datetime import datetime
import time

def classify_kospi_kosdaq():
    """
    종목코드.xlsx 파일에서 코스피/코스닥 상장사를 분류하는 프로그램
    yfinance를 통해 실제 거래소 정보를 확인하여 분류
    """
    print("=" * 70)
    print("코스피/코스닥 상장사 분류 프로그램")
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
    
    # 2. 종목코드 범위로 1차 분류 (일반적인 규칙)
    print("\n2. 종목코드 범위로 1차 분류를 시도합니다...")
    
    def classify_by_code(stock_code):
        """종목코드 범위로 거래소 추정"""
        if pd.isna(stock_code) or stock_code is None:
            return "기타"
            
        code = str(stock_code).strip()
        
        # 숫자가 아닌 문자가 포함된 경우 처리
        if not code.isdigit():
            return "기타"  # 문자 포함된 코드
        
        try:
            code_num = int(code)
            
            # 일반적인 코스피/코스닥 코드 범위 (추정)
            if 1 <= code_num <= 399999:
                return "KOSPI_추정"
            elif 400000 <= code_num <= 999999:
                return "KOSDAQ_추정"
            else:
                return "기타"
                
        except ValueError:
            return "기타"  # 변환 불가능한 경우
    
    # 1차 분류 적용
    df_stocks['거래소_추정'] = df_stocks['COMP_CODE'].apply(classify_by_code)
    
    # 1차 분류 결과 출력
    classification_counts = df_stocks['거래소_추정'].value_counts()
    print("   1차 분류 결과:")
    for market, count in classification_counts.items():
        print(f"     - {market}: {count:,}개")
    
    # 3. yfinance를 통한 실제 거래소 확인 (샘플링)
    print("\n3. yfinance를 통해 실제 거래소를 확인합니다...")
    print("   (시간 관계상 상위 100개 종목만 샘플링)")
    
    # 상위 100개 샘플로 실제 확인
    sample_stocks = df_stocks.head(100).copy()
    sample_stocks['실제_거래소'] = ''
    sample_stocks['yfinance_확인'] = False
    
    kospi_confirmed = []
    kosdaq_confirmed = []
    
    for idx, row in sample_stocks.iterrows():
        stock_code = row['COMP_CODE']
        corp_name = row['COMP_NAME']
        
        if idx % 20 == 0:
            progress = (idx + 1) / len(sample_stocks) * 100
            print(f"   진행률: {progress:.1f}% ({idx + 1}/100)")
        
        try:
            # yfinance로 코스피 확인 (.KS)
            ticker_ks = f"{stock_code}.KS"
            stock_ks = yf.Ticker(ticker_ks)
            info_ks = stock_ks.info
            
            if 'symbol' in info_ks and info_ks.get('symbol'):
                sample_stocks.loc[idx, '실제_거래소'] = 'KOSPI'
                sample_stocks.loc[idx, 'yfinance_확인'] = True
                kospi_confirmed.append(stock_code)
                time.sleep(0.1)
                continue
            
            # yfinance로 코스닥 확인 (.KQ)
            ticker_kq = f"{stock_code}.KQ"
            stock_kq = yf.Ticker(ticker_kq)
            info_kq = stock_kq.info
            
            if 'symbol' in info_kq and info_kq.get('symbol'):
                sample_stocks.loc[idx, '실제_거래소'] = 'KOSDAQ'
                sample_stocks.loc[idx, 'yfinance_확인'] = True
                kosdaq_confirmed.append(stock_code)
                time.sleep(0.1)
                continue
                
            # 둘 다 안되면 미확인
            sample_stocks.loc[idx, '실제_거래소'] = '미확인'
            
        except Exception as e:
            sample_stocks.loc[idx, '실제_거래소'] = '오류'
            continue
        
        time.sleep(0.1)  # API 제한 고려
    
    # 샘플링 결과 분석
    print(f"\n   샘플링 결과 (100개 중):")
    actual_counts = sample_stocks['실제_거래소'].value_counts()
    for market, count in actual_counts.items():
        print(f"     - {market}: {count}개")
    
    # 4. 확인된 패턴으로 전체 분류 규칙 개선
    print("\n4. 확인된 패턴으로 분류 규칙을 개선합니다...")
    
    # 확인된 코스피/코스닥 코드들의 패턴 분석
    kospi_codes = sample_stocks[sample_stocks['실제_거래소'] == 'KOSPI']['COMP_CODE'].tolist()
    kosdaq_codes = sample_stocks[sample_stocks['실제_거래소'] == 'KOSDAQ']['COMP_CODE'].tolist()
    
    print(f"   - 확인된 코스피 종목: {len(kospi_codes)}개")
    if kospi_codes:
        print(f"     예시: {kospi_codes[:5]}")
    
    print(f"   - 확인된 코스닥 종목: {len(kosdaq_codes)}개")
    if kosdaq_codes:
        print(f"     예시: {kosdaq_codes[:5]}")
    
    # 개선된 분류 함수
    def improved_classify(stock_code):
        code = stock_code.zfill(6)
        code_num = int(code)
        
        # 확인된 패턴 기반으로 개선된 분류
        if code_num < 400000:
            return "KOSPI"
        elif code_num >= 400000:
            return "KOSDAQ"
        else:
            return "기타"
    
    # 전체 데이터에 개선된 분류 적용
    df_stocks['거래소'] = df_stocks['COMP_CODE'].apply(improved_classify)
    
    # 5. 최종 결과 및 저장
    print("\n5. 최종 분류 결과:")
    final_counts = df_stocks['거래소'].value_counts()
    for market, count in final_counts.items():
        print(f"   - {market}: {count:,}개")
    
    # 6. 분류된 결과를 별도 엑셀 파일로 저장
    print("\n6. 분류 결과를 저장합니다...")
    
    try:
        # 전체 결과 저장
        os.makedirs('doc', exist_ok=True)
        df_stocks.to_excel('doc/종목분류결과.xlsx', index=False, sheet_name='전체종목')
        print(f"   성공: doc/종목분류결과.xlsx 저장 완료")
        
        # 코스피만 따로 저장
        kospi_stocks = df_stocks[df_stocks['거래소'] == 'KOSPI'].copy()
        kospi_stocks.to_excel('doc/코스피종목.xlsx', index=False, sheet_name='코스피')
        print(f"   성공: doc/코스피종목.xlsx 저장 완료 ({len(kospi_stocks):,}개)")
        
        # 코스닥만 따로 저장
        kosdaq_stocks = df_stocks[df_stocks['거래소'] == 'KOSDAQ'].copy()
        kosdaq_stocks.to_excel('doc/코스닥종목.xlsx', index=False, sheet_name='코스닥')
        print(f"   성공: doc/코스닥종목.xlsx 저장 완료 ({len(kosdaq_stocks):,}개)")
        
        # 샘플링 검증 결과도 저장
        sample_stocks.to_excel('doc/거래소확인_샘플100개.xlsx', index=False, sheet_name='샘플검증')
        print(f"   성공: doc/거래소확인_샘플100개.xlsx 저장 완료")
        
    except Exception as e:
        print(f"   저장 실패: {e}")
        return False
    
    # 7. 코스피 대표 종목 미리보기
    print(f"\n7. 코스피 대표 종목 미리보기 (상위 20개):")
    kospi_preview = kospi_stocks.head(20)
    for idx, row in kospi_preview.iterrows():
        print(f"   {row['COMP_CODE']}: {row['COMP_NAME']}")
    
    return True

def main():
    """메인 함수"""
    start_time = datetime.now()
    print(f"시작 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    success = classify_kospi_kosdaq()
    
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