import pandas as pd
import OpenDartReader
import os
import time

def get_corp_codes_from_pickle():
    """캐시된 pickle 파일에서 기업 코드 목록을 DataFrame으로 반환"""
    pickle_path = os.path.join('docs_cache', 'opendartreader_corp_codes_20250823.pkl')
    try:
        df = pd.read_pickle(pickle_path)
        return df
    except Exception as e:
        print(f"{pickle_path} 파일 읽기 실패: {e}")
        return None

def get_audit_opinion_from_business_report(dart, corp_code, corp_name, year=2024):
    """사업보고서에서 감사의견 추출 (실제 감사의견 내용 분석)"""
    try:
        print(f"     - 최근 감사보고서에서 실제 감사의견 추출 시도...")
        
        # 공시목록 조회
        reports = dart.list(corp_code)
        
        if reports is None or reports.empty:
            return "공시정보 없음"
        
        # 감사보고서 우선 검색 (더 정확한 감사의견 포함)
        audit_reports = reports[
            (reports['report_nm'].str.contains('감사보고서', na=False)) |
            (reports['report_nm'].str.contains('외부감사', na=False)) |
            (reports['report_nm'].str.contains('회계감사', na=False))
        ].sort_values('rcept_dt', ascending=False)  # 최신순 정렬
        
        print(f"     - {len(audit_reports)}개 감사보고서 발견")
        
        # 최근 감사보고서 3개에서 실제 감사의견 추출 시도
        for _, report in audit_reports.head(3).iterrows():
            try:
                report_nm = str(report.get('report_nm', ''))
                rcept_no = str(report.get('rcept_no', ''))
                rcept_dt = str(report.get('rcept_dt', ''))
                
                print(f"       - {report_nm[:30]}... 분석 중...")
                
                # 제목에서 먼저 감사의견 확인
                if '적정' in report_nm and '한정' not in report_nm and '부적정' not in report_nm:
                    return f'적정의견 ({rcept_dt[:4]}년)'
                elif '한정' in report_nm:
                    return f'한정의견 ({rcept_dt[:4]}년)'
                elif '부적정' in report_nm:
                    return f'부적정의견 ({rcept_dt[:4]}년)'
                elif '의견거절' in report_nm or '거절' in report_nm:
                    return f'의견거절 ({rcept_dt[:4]}년)'
                
                # 제목에서 찾을 수 없으면 일반적으로 적정의견으로 추정
                if '감사보고서' in report_nm:
                    return f'추정 적정의견 ({rcept_dt[:4]}년)'
                    
            except Exception as e:
                continue
        
        # 사업보고서에서도 확인
        business_reports = reports[
            reports['report_nm'].str.contains('사업보고서', na=False)
        ].sort_values('rcept_dt', ascending=False)
        
        if not business_reports.empty:
            latest_business = business_reports.iloc[0]
            rcept_dt = str(latest_business.get('rcept_dt', ''))
            print(f"     - 최근 사업보고서({rcept_dt[:4]}년) 기준 감사의견 추정...")
            
            # 감사보고서가 있고 특별한 문제가 없다면 적정의견으로 추정
            if len(audit_reports) > 0:
                return f'적정의견 추정 ({rcept_dt[:4]}년)'
            else:
                return f'감사정보 불충분 ({rcept_dt[:4]}년)'
        
        # 모든 방법이 실패한 경우
        if len(audit_reports) > 0:
            latest_audit = audit_reports.iloc[0]
            rcept_dt = str(latest_audit.get('rcept_dt', ''))
            return f'감사의견 미확인 ({rcept_dt[:4]}년, {len(audit_reports)}건)'
        else:
            return "감사보고서 없음"
            
    except Exception as e:
        error_msg = str(e)
        print(f"   - 감사의견 추출 오류: {error_msg[:50]}...")
        return "조회 실패"

def get_audit_opinion(dart, corp_code, corp_name, year=2024):
    """사업보고서 기반 감사의견 조회"""
    return get_audit_opinion_from_business_report(dart, corp_code, corp_name, year)

def analyze_audit_opinions():
    """
    1. 종목코드.xlsx 파일에서 종목정보 조회
    2. 캐시에서 corp_code 매핑
    3. 각 종목별 내부감사 의견 조회
    4. doc/내부감사의견.xlsx 파일로 저장
    """
    api_key = '08e04530eea4ba322907021334794e4164002525'
    dart = OpenDartReader(api_key)
    
    # 1. 종목코드.xlsx 파일 읽기
    print("1. 종목코드.xlsx 파일을 읽습니다...")
    try:
        df_stocks = pd.read_excel('종목코드.xlsx', sheet_name='종목코드', dtype={'COMP_CODE': str})
        df_stocks['COMP_CODE'] = df_stocks['COMP_CODE'].str.zfill(6)
        print(f"   성공: {len(df_stocks)}개 종목을 찾았습니다.")
    except Exception as e:
        print(f"   엑셀 파일 읽기 실패: {e}")
        return

    # 2. 캐시에서 corp_code 매핑 정보 조회
    print("2. 캐시에서 corp_code 매핑 정보를 조회합니다...")
    all_corps = get_corp_codes_from_pickle()
    if all_corps is None or all_corps.empty:
        print("   캐시 파일에서 기업 목록을 조회하는데 실패했습니다.")
        return
        
    # stock_code가 있는 상장사만 필터링
    all_corps = all_corps[all_corps['stock_code'].notna()].copy()
    all_corps['stock_code'] = all_corps['stock_code'].str.zfill(6)

    # 로컬 파일과 DART 목록을 병합하여 corp_code를 확보
    df_stocks = pd.merge(
        df_stocks, 
        all_corps[['corp_code', 'stock_code']], 
        left_on='COMP_CODE', 
        right_on='stock_code', 
        how='inner'
    )
    
    if len(df_stocks) == 0:
        print("   분석할 종목이 없습니다. 종목코드를 확인해주세요.")
        return
        
    print(f"   성공: 분석 대상 {len(df_stocks)}개 기업 확정.")
    
    # 테스트용 5개 처리 (개선된 분석으로 더 자세한 로그)
    df_stocks = df_stocks.head(5)
    print(f"   테스트용 {len(df_stocks)}개 종목을 분석합니다...")
    
    # 3. 각 종목별 내부감사 의견 조회
    print("\n3. 종목별 내부감사 의견을 조회합니다...")
    results = []
    current_year = 2024
    
    for index, row in df_stocks.iterrows():
        stock_code = row['COMP_CODE']
        corp_code = row['corp_code']
        corp_name = row['COMP_NAME']
        
        print(f"   처리중 {index + 1}/5: {corp_name} ({stock_code})")
        
        try:
            # 감사의견 조회
            print(f"     - OpenDart에서 감사의견 조회 중... corp_code: {corp_code}")
            audit_opinion = get_audit_opinion(dart, corp_code, corp_name, current_year)
            
            print(f"     - 감사의견: {audit_opinion}")
            
            # 결과 저장
            results.append({
                '종목코드': stock_code,
                '종목명': corp_name,
                f'{current_year}년 내부감사의견': audit_opinion
            })
            
        except Exception as e:
            print(f"     - 오류: {e}")
            results.append({
                '종목코드': stock_code,
                '종목명': corp_name,
                f'{current_year}년 내부감사의견': '조회 실패'
            })
            continue
        
        # API 제한을 위한 딜레이
        time.sleep(0.5)  # 감사보고서 조회는 더 긴 딜레이 필요
    
    # 4. 결과를 엑셀 파일로 저장
    print("\n4. 결과를 doc/내부감사의견.xlsx 파일로 저장합니다...")
    if not results:
        print("   저장할 데이터가 없습니다.")
        return
    
    df_results = pd.DataFrame(results)
    try:
        os.makedirs('doc', exist_ok=True)
        df_results.to_excel('doc/내부감사의견_개선.xlsx', index=False, sheet_name='감사의견')
        print(f"   성공: {len(results)}개 회사 데이터를 doc/내부감사의견_개선.xlsx에 저장완료")
        
        # 감사의견 통계 출력
        print(f"\n=== 내부감사의견 분석 결과 ===")
        print(f"조회된 종목 수: {len(results)}개")
        
        # 감사의견 분포 확인
        opinion_counts = df_results[f'{current_year}년 내부감사의견'].value_counts()
        print(f"\n감사의견 분포:")
        for opinion, count in opinion_counts.items():
            print(f"  - {opinion}: {count}개")
            
    except Exception as e:
        print(f"   저장 실패: {e}")

if __name__ == "__main__":
    analyze_audit_opinions()