import pandas as pd
import OpenDartReader
import os
import time
import re

def get_corp_codes_from_pickle():
    """캐시된 pickle 파일에서 기업 코드 목록을 DataFrame으로 반환"""
    pickle_path = os.path.join('docs_cache', 'opendartreader_corp_codes_20250823.pkl')
    try:
        df = pd.read_pickle(pickle_path)
        return df
    except Exception as e:
        print(f"{pickle_path} 파일 읽기 실패: {e}")
        return None

def get_internal_control_evaluation(dart, corp_code, corp_name, year=2024):
    """사업보고서에서 내부회계관리제도 효과성 평가 결과 추출"""
    try:
        print(f"     - 사업보고서 목록에서 내부통제 관련 정보 추출 시도...")
        
        # 공시목록 조회 (사업보고서 찾기)
        try:
            reports = dart.list(corp_code)
            if reports is None or reports.empty:
                return "공시정보 없음"
        except Exception as e:
            print(f"     - 공시목록 조회 오류: {str(e)[:50]}...")
            return "조회 실패"
        
        # 사업보고서 필터링
        business_reports = reports[
            reports['report_nm'].str.contains('사업보고서', na=False)
        ].sort_values('rcept_dt', ascending=False)
        
        print(f"     - {len(business_reports)}개 사업보고서 발견")
        
        if business_reports.empty:
            return "사업보고서 없음"
        
        # 최신 사업보고서에서 내부통제 관련 정보 검색
        latest_business = business_reports.iloc[0]
        report_nm = str(latest_business.get('report_nm', ''))
        rcept_dt = str(latest_business.get('rcept_dt', ''))
        rcept_no = str(latest_business.get('rcept_no', ''))
        
        print(f"     - 최신 사업보고서 분석: {report_nm[:30]}... ({rcept_dt[:4]}년)")
        
        # 실제 사업보고서 내용 조회 시도
        try:
            print(f"     - 사업보고서 상세 내용 조회 시도... rcept_no: {rcept_no}")
            # OpenDartReader의 document 메서드를 사용하여 실제 내용 조회
            document_content = dart.document(rcept_no)
            
            if document_content is not None and not document_content.empty:
                print(f"     - 문서 내용 조회 성공, {len(document_content)}개 항목 분석...")
                
                # 내부통제 관련 섹션 찾기
                internal_control_sections = []
                for idx, row in document_content.iterrows():
                    title = str(row.get('title', ''))
                    content = str(row.get('content', ''))
                    
                    # 내부통제 관련 제목 검색
                    if any(keyword in title for keyword in ['내부통제', '내부회계', '회계관리제도']):
                        internal_control_sections.append({
                            'title': title,
                            'content': content[:500]  # 처음 500자만
                        })
                        print(f"       - 내부통제 관련 섹션 발견: {title[:40]}...")
                
                # 내부통제 섹션에서 효과성 평가 결과 분석
                if internal_control_sections:
                    for section in internal_control_sections:
                        content = section['content']
                        title = section['title']
                        
                        # 효과성 평가 결과 키워드 검색
                        if '효과적' in content or '효과적' in title:
                            return f"효과적 ({rcept_dt[:4]}년)"
                        elif '적정' in content and '부적정' not in content:
                            return f"적정 ({rcept_dt[:4]}년)"
                        elif '양호' in content:
                            return f"양호 ({rcept_dt[:4]}년)"
                        elif '미흡' in content:
                            return f"미흡 ({rcept_dt[:4]}년)"
                        elif '부적정' in content:
                            return f"부적정 ({rcept_dt[:4]}년)"
                        elif '중요한 취약점' in content:
                            return f"중요한 취약점 존재 ({rcept_dt[:4]}년)"
                        elif '개선' in content:
                            return f"개선필요 ({rcept_dt[:4]}년)"
                        elif '보완' in content:
                            return f"보완필요 ({rcept_dt[:4]}년)"
                    
                    # 내부통제 섹션은 있지만 구체적 평가 결과를 찾을 수 없는 경우
                    return f"내부회계관리제도 운영 확인됨 ({rcept_dt[:4]}년)"
                else:
                    print(f"     - 내부통제 관련 섹션을 찾을 수 없음")
                    
        except Exception as e:
            print(f"     - 문서 내용 조회 실패: {str(e)[:50]}...")
        
        # 보고서 제목에서 특별한 문제 징후 확인
        problem_indicators = ['정정', '감리', '제재', '위반', '불성실']
        has_problem = any(indicator in report_nm for indicator in problem_indicators)
        
        if has_problem:
            return f"내부통제 관련 문제 가능성 ({rcept_dt[:4]}년)"
        
        # 정상적인 사업보고서라면 일반적으로 내부회계관리제도가 운영되고 있음
        if '사업보고서' in report_nm:
            return f"내부회계관리제도 운영 추정 ({rcept_dt[:4]}년)"
        
        return "내부회계관리제도 정보 확인 필요"
        
    except Exception as e:
        error_msg = str(e)
        print(f"   - 내부통제 정보 추출 오류: {error_msg[:50]}...")
        return "조회 실패"

def analyze_internal_control():
    """
    1. 종목코드.xlsx 파일에서 종목정보 조회
    2. 캐시에서 corp_code 매핑
    3. 각 종목별 내부회계관리제도 효과성 평가 결과 조회
    4. doc/내부회계관리제도평가.xlsx 파일로 저장
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
    
    # 테스트용 5개 처리
    df_stocks = df_stocks.head(5)
    print(f"   테스트용 {len(df_stocks)}개 종목을 분석합니다...")
    
    # 3. 각 종목별 내부회계관리제도 평가 결과 조회
    print("\n3. 종목별 내부회계관리제도 효과성 평가를 조회합니다...")
    results = []
    current_year = 2024
    
    for index, row in df_stocks.iterrows():
        stock_code = row['COMP_CODE']
        corp_code = row['corp_code']
        corp_name = row['COMP_NAME']
        
        print(f"   처리중 {index + 1}/5: {corp_name} ({stock_code})")
        
        try:
            # 내부회계관리제도 평가 결과 조회
            print(f"     - OpenDart에서 내부통제 정보 조회 중... corp_code: {corp_code}")
            internal_control_result = get_internal_control_evaluation(dart, corp_code, corp_name, current_year)
            
            print(f"     - 내부회계관리제도 평가: {internal_control_result}")
            
            # 결과 저장
            results.append({
                '종목코드': stock_code,
                '종목명': corp_name,
                f'{current_year}년 내부회계관리제도 평가': internal_control_result
            })
            
        except Exception as e:
            print(f"     - 오류: {e}")
            results.append({
                '종목코드': stock_code,
                '종목명': corp_name,
                f'{current_year}년 내부회계관리제도 평가': '조회 실패'
            })
            continue
        
        # API 제한을 위한 딜레이
        time.sleep(1.0)  # 사업보고서 조회는 더 긴 딜레이 필요
    
    # 4. 결과를 엑셀 파일로 저장
    print("\n4. 결과를 doc/내부회계관리제도평가.xlsx 파일로 저장합니다...")
    if not results:
        print("   저장할 데이터가 없습니다.")
        return
    
    df_results = pd.DataFrame(results)
    try:
        os.makedirs('doc', exist_ok=True)
        df_results.to_excel('doc/내부회계관리제도평가.xlsx', index=False, sheet_name='내부회계관리제도')
        print(f"   성공: {len(results)}개 회사 데이터를 doc/내부회계관리제도평가.xlsx에 저장완료")
        
        # 평가 결과 통계 출력
        print(f"\n=== 내부회계관리제도 평가 결과 ==")
        print(f"조회된 종목 수: {len(results)}개")
        
        # 평가 결과 분포 확인
        evaluation_counts = df_results[f'{current_year}년 내부회계관리제도 평가'].value_counts()
        print(f"\n평가 결과 분포:")
        for evaluation, count in evaluation_counts.items():
            print(f"  - {evaluation}: {count}개")
            
    except Exception as e:
        print(f"   저장 실패: {e}")

if __name__ == "__main__":
    analyze_internal_control()