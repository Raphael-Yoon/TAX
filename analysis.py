
import pandas as pd
import OpenDartReader
import yfinance as yf
import os

def get_stock_data_from_yfinance(stock_code):
    """yfinance를 사용하여 PBR 정보 조회"""
    try:
        ticker = f"{stock_code}.KS"
        stock = yf.Ticker(ticker)
        info = stock.info
        
        pbr = info.get('priceToBook')
        return pbr
    except Exception as e:
        return None

def calculate_growth_rate(current, previous):
    """전년도 대비 증가율 계산"""
    if previous and previous != 0:
        return ((current - previous) / previous) * 100
    return None

def get_latest_corp_list():
    """OpenDartReader로 최신 상장회사 목록 조회"""
    try:
        api_key = '08e04530eea4ba322907021334794e4164002525'
        dart = OpenDartReader(api_key)
        
        corp_list = dart.list()
        
        if corp_list is None or corp_list.empty:
            return None
            
        # KOSPI, KOSDAQ 상장 주식만 필터링
        if 'stock_code' in corp_list.columns:
            listed_stocks = corp_list[
                (corp_list['stock_code'].notna()) & 
                (corp_list['corp_cls'].isin(['Y', 'K']))
            ].copy()
        else:
            return None
        
        return listed_stocks
        
    except Exception as e:
        print(f"최신 종목정보 조회 실패: {e}")
        return None

def get_corp_codes_from_pickle():
    """캐시된 pickle 파일에서 기업 코드 목록을 DataFrame으로 반환"""
    pickle_path = os.path.join('docs_cache', 'opendartreader_corp_codes_20250823.pkl')
    try:
        df = pd.read_pickle(pickle_path)
        return df
    except Exception as e:
        print(f"{pickle_path} 파일 읽기 실패: {e}")
        return None

def calculate_operating_profit_growth():
    """
    1. 종목코드.xlsx 파일에서 종목정보 조회
    2. 캐시에서 corp_code 매핑
    3. 각 종목별 전년도대비 영업이익률 계산
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
    
    # 전체 데이터 처리
    print(f"   전체 {len(df_stocks)}개 종목을 분석합니다...")
    
    # 3. 각 종목별 영업이익률 계산
    print("\n3. 종목별 재무분석을 진행합니다...")
    current_year = 2024
    results = []
    
    for index, row in df_stocks.iterrows():
        stock_code = row['COMP_CODE']
        corp_code = row['corp_code']
        corp_name = row['COMP_NAME']
        
        print(f"   처리중 {index + 1}/{len(df_stocks)}: {corp_name} ({stock_code})")
        
        try:
            # 재무제표 데이터 조회
            print(f"     - OpenDart API 조회 중... corp_code: {corp_code}")
            fs_current = dart.finstate(corp_code, current_year)
            fs_previous = dart.finstate(corp_code, current_year - 1)
            
            if fs_current is None or fs_previous is None:
                print(f"     - 재무데이터 조회 실패")
                continue
                
            print(f"     - 재무데이터 조회 성공")
            
            # 영업이익 추출
            op_current_data = fs_current[
                (fs_current["account_nm"] == '영업이익') & 
                (fs_current["fs_nm"] == '연결재무제표')
            ]["thstrm_amount"]
            
            op_previous_data = fs_previous[
                (fs_previous["account_nm"] == '영업이익') & 
                (fs_previous["fs_nm"] == '연결재무제표')
            ]["thstrm_amount"]
            
            # 당기순이익 추출
            ni_current_data = fs_current[
                (fs_current["account_nm"] == '당기순이익') & 
                (fs_current["fs_nm"] == '연결재무제표')
            ]["thstrm_amount"]
            
            ni_previous_data = fs_previous[
                (fs_previous["account_nm"] == '당기순이익') & 
                (fs_previous["fs_nm"] == '연결재무제표')
            ]["thstrm_amount"]
            
            # 매출액 추출
            revenue_current_data = fs_current[
                (fs_current["account_nm"] == '매출액') & 
                (fs_current["fs_nm"] == '연결재무제표')
            ]["thstrm_amount"]
            
            revenue_previous_data = fs_previous[
                (fs_previous["account_nm"] == '매출액') & 
                (fs_previous["fs_nm"] == '연결재무제표')
            ]["thstrm_amount"]
            
            # 부채총계 추출 (부채비율 계산용)
            debt_current_data = fs_current[
                (fs_current["account_nm"] == '부채총계') & 
                (fs_current["fs_nm"] == '연결재무제표')
            ]["thstrm_amount"]
            
            # 데이터 체크
            op_success = len(op_current_data) > 0 and len(op_previous_data) > 0
            ni_success = len(ni_current_data) > 0 and len(ni_previous_data) > 0
            revenue_success = len(revenue_current_data) > 0 and len(revenue_previous_data) > 0
            debt_success = len(debt_current_data) > 0
            
            if not op_success and not ni_success and not revenue_success:
                print(f"     - 영업이익/순이익/매출액 데이터 모두 없음")
                continue
            
            # 변수 초기화
            op_current, op_prev, op_growth_rate = None, None, None
            ni_current, ni_prev, ni_growth_rate = None, None, None
            revenue_current, revenue_prev, revenue_growth_rate = None, None, None
            equity, debt_total, market_cap, pbr = None, None, None, None
            operating_cash_flow = None
            per, roe, debt_ratio = None, None, None
            
            # 영업이익률 계산
            if op_success:
                op_current = int(str(op_current_data.iloc[0]).replace(',', ''))
                op_prev = int(str(op_previous_data.iloc[0]).replace(',', ''))
                op_growth_rate = calculate_growth_rate(op_current, op_prev)
                
                if op_growth_rate is not None:
                    print(f"     - 영업이익률: {op_growth_rate:.2f}%")
                    print(f"       2024년: {op_current:,}원, 2023년: {op_prev:,}원")
            else:
                print(f"     - 영업이익 데이터 없음")
            
            # 순이익률 계산
            if ni_success:
                ni_current = int(str(ni_current_data.iloc[0]).replace(',', ''))
                ni_prev = int(str(ni_previous_data.iloc[0]).replace(',', ''))
                ni_growth_rate = calculate_growth_rate(ni_current, ni_prev)
                
                if ni_growth_rate is not None:
                    print(f"     - 순이익률: {ni_growth_rate:.2f}%")
                    print(f"       2024년: {ni_current:,}원, 2023년: {ni_prev:,}원")
            else:
                print(f"     - 순이익 데이터 없음")
            
            # 매출변동률 계산
            if revenue_success:
                revenue_current = int(str(revenue_current_data.iloc[0]).replace(',', ''))
                revenue_prev = int(str(revenue_previous_data.iloc[0]).replace(',', ''))
                revenue_growth_rate = calculate_growth_rate(revenue_current, revenue_prev)
                
                if revenue_growth_rate is not None:
                    print(f"     - 매출변동률: {revenue_growth_rate:.2f}%")
                    print(f"       2024년: {revenue_current:,}원, 2023년: {revenue_prev:,}원")
            else:
                print(f"     - 매출액 데이터 없음")
            
            # PBR 계산 (시가총액 / 자본총계)
            # 영업현금흐름 계산
            print(f"     - 영업현금흐름 조회 중...")
            try:
                # 현금흐름표 조회
                cf_current = dart.finstate_all(corp_code, current_year)
                if cf_current is not None and not cf_current.empty:
                    # 영업활동으로인한현금흐름 추출
                    ocf_data = cf_current[
                        (cf_current["account_nm"].str.contains('영업활동', na=False)) & 
                        (cf_current["fs_nm"] == '연결현금흐름표')
                    ]["thstrm_amount"]
                    
                    if len(ocf_data) > 0:
                        operating_cash_flow = int(str(ocf_data.iloc[0]).replace(',', ''))
                        print(f"     - 영업현금흐름: {operating_cash_flow:,}원")
                    else:
                        print(f"     - 영업현금흐름 데이터 없음")
                        operating_cash_flow = None
                else:
                    print(f"     - 현금흐름표 조회 실패")
                    operating_cash_flow = None
            except Exception as e:
                print(f"     - 영업현금흐름 조회 오류: {str(e)[:30]}...")
                operating_cash_flow = None
            
            print(f"     - PBR 계산 중...")
            
            # 자본총계 (자기자본) 추출
            equity_current_data = fs_current[
                (fs_current["account_nm"] == '자본총계') & 
                (fs_current["fs_nm"] == '연결재무제표')
            ]["thstrm_amount"]
            
            # 부채총계 추출
            if debt_success:
                debt_total = int(str(debt_current_data.iloc[0]).replace(',', ''))
                print(f"     - 부채총계: {debt_total:,}원")
            else:
                print(f"     - 부채총계 데이터 없음")
                debt_total = None
            
            if len(equity_current_data) > 0:
                equity = int(str(equity_current_data.iloc[0]).replace(',', ''))
                print(f"     - 자기자본: {equity:,}원")
                
                # yfinance에서 시가총액 조회
                try:
                    ticker = f"{stock_code}.KS"
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    market_cap = info.get('marketCap')
                    
                    if market_cap and equity > 0:
                        pbr = market_cap / equity
                        print(f"     - PBR: {pbr:.2f} (시가총액: {market_cap:,}, 자본총계: {equity:,})")
                        
                        # PER 계산 (시가총액 / 당기순이익)
                        if ni_current and ni_current > 0:
                            per = market_cap / ni_current
                            print(f"     - PER: {per:.2f} (시가총액: {market_cap:,}, 당기순이익: {ni_current:,})")
                        else:
                            print(f"     - PER 계산 불가 (당기순이익이 0 이하)")
                            per = None
                        
                        # ROE 계산 (당기순이익 / 자기자본 * 100)
                        if ni_current and equity > 0:
                            roe = (ni_current / equity) * 100
                            print(f"     - ROE: {roe:.2f}% (당기순이익: {ni_current:,}, 자기자본: {equity:,})")
                        else:
                            print(f"     - ROE 계산 불가")
                            roe = None
                            
                        # 부채비율 계산 (부채총계 / 자기자본 * 100)
                        if debt_total and equity > 0:
                            debt_ratio = (debt_total / equity) * 100
                            print(f"     - 부채비율: {debt_ratio:.2f}% (부채총계: {debt_total:,}, 자기자본: {equity:,})")
                        else:
                            print(f"     - 부채비율 계산 불가")
                            debt_ratio = None
                            
                    else:
                        print(f"     - PBR 계산 불가 (시가총액: {market_cap}, 자본총액: {equity})")
                        pbr = None
                except Exception as e:
                    print(f"     - 주가 데이터 조회 오류: {e}")
                    pbr = None
            else:
                print(f"     - 자본총계 데이터 없음으로 PBR/ROE/부채비율 계산 불가")
                pbr = None
            
            # 결과 저장
            results.append({
                '종목코드': stock_code,
                '종목명': corp_name,
                '2024년 영업이익': op_current,
                '2023년 영업이익': op_prev,
                '전년도대비 영업이익률(%)': round(op_growth_rate, 2) if op_growth_rate is not None else None,
                '2024년 당기순이익': ni_current,
                '2023년 당기순이익': ni_prev,
                '전년도대비 순이익률(%)': round(ni_growth_rate, 2) if ni_growth_rate is not None else None,
                '2024년 매출액': revenue_current,
                '2023년 매출액': revenue_prev,
                '전년도대비 매출변동률(%)': round(revenue_growth_rate, 2) if revenue_growth_rate is not None else None,
                '2024년 자기자본': equity,
                '2024년 부채총계': debt_total,
                '2024년 영업현금흐름': operating_cash_flow,
                '시가총액': market_cap,
                'PBR': round(pbr, 2) if pbr is not None else None,
                'PER': round(per, 2) if per is not None else None,
                'ROE(%)': round(roe, 2) if roe is not None else None,
                '부채비율(%)': round(debt_ratio, 2) if debt_ratio is not None else None
            })
            
        except Exception as e:
            print(f"     - 오류: {e}")
            continue
        
        # API 제한을 위한 딜레이 및 중간 저장
        import time
        time.sleep(0.1)
        
        # 50개마다 중간 저장 및 진행상황 출력
        if len(results) > 0 and len(results) % 50 == 0:
            try:
                df_temp = pd.DataFrame(results)
                os.makedirs('doc', exist_ok=True)
                df_temp.to_excel(f'doc/종목분석결과_임시_{len(results)}개.xlsx', index=False, sheet_name='종목분석')
                
                # 진행 상황 및 통계 출력
                progress_percent = (index + 1) / len(df_stocks) * 100
                print(f"\n=== 진행 상황: {len(results)}개 완료 ({progress_percent:.1f}%) ===")
                
                # 현재까지 통계
                if len(results) >= 5:  # 최소 5개 이상일 때만 통계 출력
                    valid_op_rates = [r['전년도대비 영업이익률(%)'] for r in results if r['전년도대비 영업이익률(%)'] is not None]
                    valid_pbrs = [r['PBR'] for r in results if r['PBR'] is not None]
                    valid_roes = [r['ROE(%)'] for r in results if r['ROE(%)'] is not None]
                    
                    if valid_op_rates:
                        avg_op = sum(valid_op_rates) / len(valid_op_rates)
                        print(f"현재까지 평균 영업이익률: {avg_op:.2f}%")
                    if valid_pbrs:
                        avg_pbr = sum(valid_pbrs) / len(valid_pbrs)
                        print(f"현재까지 평균 PBR: {avg_pbr:.2f}")
                    if valid_roes:
                        avg_roe = sum(valid_roes) / len(valid_roes)
                        print(f"현재까지 평균 ROE: {avg_roe:.2f}%")
                    
                print(f"중간저장 완료: doc/종목분석결과_임시_{len(results)}개.xlsx")
                print("=" * 60)
                
            except Exception as e:
                print(f"     중간저장 실패: {e}")
        
        # 10개마다 간단한 진행상황 출력
        elif (index + 1) % 10 == 0:
            progress_percent = (index + 1) / len(df_stocks) * 100
            print(f"   >>> 진행률: {progress_percent:.1f}% ({index + 1}/{len(df_stocks)}개 처리 중, {len(results)}개 성공)")
    
    # 4. 결과를 엑셀 파일로 저장
    print("\n4. 결과를 doc/종목분석결과.xlsx 파일로 저장합니다...")
    if not results:
        print("   저장할 데이터가 없습니다.")
        return
    
    df_results = pd.DataFrame(results)
    try:
        os.makedirs('doc', exist_ok=True)
        df_results.to_excel('doc/종목분석결과.xlsx', index=False, sheet_name='종목분석')
        print(f"   성공: {len(results)}개 회사 데이터를 doc/종목분석결과.xlsx에 저장완료")
        
        # 요약 통계 출력
        print(f"\n=== 분석 결과 요약 ===")
        print(f"분석된 종목 수: {len(results)}개")
        
        # 영업이익률 통계
        valid_op_rates = [r['전년도대비 영업이익률(%)'] for r in results if r['전년도대비 영업이익률(%)'] is not None]
        if valid_op_rates:
            avg_op_rate = sum(valid_op_rates) / len(valid_op_rates)
            print(f"평균 영업이익률: {avg_op_rate:.2f}%")
        
        # 순이익률 통계
        valid_ni_rates = [r['전년도대비 순이익률(%)'] for r in results if r['전년도대비 순이익률(%)'] is not None]
        if valid_ni_rates:
            avg_ni_rate = sum(valid_ni_rates) / len(valid_ni_rates)
            print(f"평균 순이익률: {avg_ni_rate:.2f}%")
        
        # 매출변동률 통계
        valid_revenue_rates = [r['전년도대비 매출변동률(%)'] for r in results if r['전년도대비 매출변동률(%)'] is not None]
        if valid_revenue_rates:
            avg_revenue_rate = sum(valid_revenue_rates) / len(valid_revenue_rates)
            print(f"평균 매출변동률: {avg_revenue_rate:.2f}%")
        
        # PBR 통계
        valid_pbrs = [r['PBR'] for r in results if r['PBR'] is not None]
        if valid_pbrs:
            avg_pbr = sum(valid_pbrs) / len(valid_pbrs)
            print(f"평균 PBR: {avg_pbr:.2f}")
        
        # 영업현금흐름 통계
        valid_ocf = [r['2024년 영업현금흐름'] for r in results if r['2024년 영업현금흐름'] is not None]
        if valid_ocf:
            avg_ocf = sum(valid_ocf) / len(valid_ocf)
            print(f"평균 영업현금흐름: {avg_ocf:,.0f}원")
        
        # PER 통계
        valid_per = [r['PER'] for r in results if r['PER'] is not None]
        if valid_per:
            avg_per = sum(valid_per) / len(valid_per)
            print(f"평균 PER: {avg_per:.2f}")
        
        # ROE 통계
        valid_roe = [r['ROE(%)'] for r in results if r['ROE(%)'] is not None]
        if valid_roe:
            avg_roe = sum(valid_roe) / len(valid_roe)
            print(f"평균 ROE: {avg_roe:.2f}%")
        
        # 부채비율 통계
        valid_debt_ratio = [r['부채비율(%)'] for r in results if r['부채비율(%)'] is not None]
        if valid_debt_ratio:
            avg_debt_ratio = sum(valid_debt_ratio) / len(valid_debt_ratio)
            print(f"평균 부채비율: {avg_debt_ratio:.2f}%")
            
    except Exception as e:
        print(f"   저장 실패: {e}")

if __name__ == "__main__":
    calculate_operating_profit_growth()
