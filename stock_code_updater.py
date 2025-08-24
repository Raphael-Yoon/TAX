import pandas as pd
import OpenDartReader
import os
from datetime import datetime
import time

def get_latest_stock_codes():
    """
    OpenDartReader를 사용하여 최신 상장회사 목록을 조회하고 
    종목코드.xlsx 파일을 업데이트하는 프로그램
    """
    api_key = '08e04530eea4ba322907021334794e4164002525'
    dart = OpenDartReader(api_key)
    
    print("=" * 60)
    print("최신 종목코드 업데이트 프로그램")
    print("=" * 60)
    
    # 1. 캐시된 파일에서 기업 목록 조회 (OpenDart API 대신)
    print("\n1. 캐시된 파일에서 기업 목록을 조회합니다...")
    try:
        # 캐시 파일 경로 확인
        cache_dir = 'docs_cache'
        cache_files = []
        if os.path.exists(cache_dir):
            for file in os.listdir(cache_dir):
                if file.startswith('opendartreader_corp_codes') and file.endswith('.pkl'):
                    cache_files.append(file)
        
        if not cache_files:
            print("   캐시 파일이 없습니다. 직접 API 호출을 시도합니다...")
            # 직접 기업정보 조회 (corp_code 없이)
            corp_list = dart.company()
            if corp_list is None or corp_list.empty:
                print("   실패: 기업 목록을 조회할 수 없습니다.")
                return False
        else:
            # 가장 최신 캐시 파일 사용
            latest_cache = max(cache_files)
            cache_path = os.path.join(cache_dir, latest_cache)
            print(f"   - 캐시 파일 사용: {latest_cache}")
            corp_list = pd.read_pickle(cache_path)
        
        print(f"   성공: 총 {len(corp_list)}개 기업 정보를 조회했습니다.")
        
    except Exception as e:
        print(f"   오류: 기업 목록 조회 실패 - {e}")
        return False
    
    # 2. 상장된 주식(KOSPI, KOSDAQ)만 필터링
    print("\n2. 상장된 주식만 필터링합니다...")
    
    print(f"   - 전체 기업 수: {len(corp_list)}개")
    print("   - 컬럼 정보:", list(corp_list.columns))
    
    # stock_code가 있는 상장사만 필터링
    if 'stock_code' in corp_list.columns:
        # stock_code가 있는 상장사만 (corp_cls가 없으므로 stock_code 존재 여부로만 판단)
        listed_stocks = corp_list[
            corp_list['stock_code'].notna()
        ].copy()
        
        print(f"   - stock_code가 있는 상장사: {len(listed_stocks)}개")
        
        # 필요한 컬럼만 선택하고 정리
        if not listed_stocks.empty:
            # stock_code를 6자리로 패딩
            listed_stocks['stock_code'] = listed_stocks['stock_code'].str.zfill(6)
            
            # 필요한 컬럼만 선택
            result_df = listed_stocks[['stock_code', 'corp_name', 'corp_code']].copy()
            result_df.columns = ['COMP_CODE', 'COMP_NAME', 'CORP_CODE']
            
            # 종목코드 순으로 정렬
            result_df = result_df.sort_values('COMP_CODE').reset_index(drop=True)
            
            print(f"   성공: {len(result_df)}개 상장사 정리 완료")
            
        else:
            print("   실패: 상장사 데이터가 없습니다.")
            return False
            
    else:
        print("   오류: stock_code 컬럼이 없습니다.")
        print("   사용 가능한 컬럼:", list(corp_list.columns))
        return False
    
    # 3. 기존 파일과 비교
    print("\n3. 기존 종목코드.xlsx 파일과 비교합니다...")
    existing_file = '종목코드.xlsx'
    
    if os.path.exists(existing_file):
        try:
            old_df = pd.read_excel(existing_file, sheet_name='종목코드')
            print(f"   - 기존 파일: {len(old_df)}개 종목")
            print(f"   - 새로운 데이터: {len(result_df)}개 종목")
            
            # 새로 추가된 종목과 삭제된 종목 확인
            if 'COMP_CODE' in old_df.columns:
                old_codes = set(old_df['COMP_CODE'].astype(str).str.zfill(6))
                new_codes = set(result_df['COMP_CODE'])
                
                added_codes = new_codes - old_codes
                removed_codes = old_codes - new_codes
                
                print(f"   - 새로 추가된 종목: {len(added_codes)}개")
                if len(added_codes) > 0 and len(added_codes) <= 10:
                    for code in list(added_codes)[:10]:
                        name = result_df[result_df['COMP_CODE'] == code]['COMP_NAME'].iloc[0]
                        print(f"     * {code}: {name}")
                
                print(f"   - 삭제된 종목: {len(removed_codes)}개")
                if len(removed_codes) > 0 and len(removed_codes) <= 10:
                    for code in list(removed_codes)[:10]:
                        if code in old_df['COMP_CODE'].astype(str).str.zfill(6).values:
                            idx = old_df[old_df['COMP_CODE'].astype(str).str.zfill(6) == code].index[0]
                            name = old_df.loc[idx, 'COMP_NAME']
                            print(f"     * {code}: {name}")
            
        except Exception as e:
            print(f"   기존 파일 읽기 실패: {e}")
    else:
        print("   기존 파일이 없습니다. 새로 생성합니다.")
    
    # 4. 새로운 파일 저장
    print("\n4. 업데이트된 종목코드.xlsx 파일을 저장합니다...")
    
    try:
        # 백업 파일 생성 (기존 파일이 있는 경우)
        if os.path.exists(existing_file):
            backup_filename = f"종목코드_백업_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            import shutil
            shutil.copy2(existing_file, backup_filename)
            print(f"   - 백업 파일 생성: {backup_filename}")
        
        # 새 파일 저장
        result_df.to_excel(existing_file, index=False, sheet_name='종목코드')
        print(f"   성공: 종목코드.xlsx 파일 업데이트 완료")
        print(f"   - 총 종목 수: {len(result_df)}개")
        
        # 상위 10개 종목 미리보기
        print("\n5. 업데이트된 종목 목록 미리보기 (상위 10개):")
        for i, row in result_df.head(10).iterrows():
            print(f"   {i+1:2d}. {row['COMP_CODE']}: {row['COMP_NAME']}")
        print("   ...")
        
        return True
        
    except Exception as e:
        print(f"   저장 실패: {e}")
        return False

def update_corp_codes_cache():
    """기업코드 캐시 파일도 업데이트"""
    print("\n6. 기업코드 캐시 파일을 업데이트합니다...")
    
    api_key = '08e04530eea4ba322907021334794e4164002525'
    dart = OpenDartReader(api_key)
    
    try:
        # 전체 기업 목록 다시 조회 (캐시용)
        corp_list = dart.list()
        
        if corp_list is not None and not corp_list.empty:
            # 캐시 디렉토리 생성
            cache_dir = 'docs_cache'
            os.makedirs(cache_dir, exist_ok=True)
            
            # 오늘 날짜로 캐시 파일명 생성
            today = datetime.now().strftime('%Y%m%d')
            cache_filename = f'opendartreader_corp_codes_{today}.pkl'
            cache_path = os.path.join(cache_dir, cache_filename)
            
            # 캐시 파일 저장
            corp_list.to_pickle(cache_path)
            print(f"   성공: 캐시 파일 저장 완료 - {cache_path}")
            print(f"   - 총 기업 수: {len(corp_list)}개")
            
            return True
        else:
            print("   실패: 기업 목록을 가져올 수 없습니다.")
            return False
            
    except Exception as e:
        print(f"   캐시 업데이트 실패: {e}")
        return False

def main():
    """메인 함수"""
    print(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 최신 종목코드 업데이트
    success1 = get_latest_stock_codes()
    
    # 약간의 딜레이 후 캐시 업데이트
    time.sleep(1)
    success2 = update_corp_codes_cache()
    
    print("\n" + "=" * 60)
    if success1 and success2:
        print("[SUCCESS] 종목코드 업데이트가 성공적으로 완료되었습니다!")
    else:
        print("[ERROR] 일부 작업이 실패했습니다.")
    
    print(f"완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

if __name__ == "__main__":
    main()