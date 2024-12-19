import capnp
import tempfile
import os

# 스키마 문자열 정의
schema_text = """
@0xb288fdf71fc6df6a;  # 유니크 ID

struct MarketData {
  source @0 :Text;
  timestamp @1 :Text;
  dataType @2 :Text;
  content @3 :List(PriceData);
}

struct PriceData {
  itemCode @0 :Text;
  trdTime @1 :Text;
  currentPrice @2 :Float64;
}
"""

# 임시 파일에 스키마 저장
with tempfile.NamedTemporaryFile(suffix='.capnp', delete=False) as f:
    f.write(schema_text.encode())
    schema_path = f.name

# 스키마 로드
MarketData = capnp.load(schema_path)

# 임시 파일 삭제
os.unlink(schema_path)