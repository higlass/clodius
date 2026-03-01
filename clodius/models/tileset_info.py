from typing import List, Optional

from pydantic import BaseModel


class TilesetInfo(BaseModel):
	max_width: int
	min_pos: List[int]
	max_pos: List[int]
	chromsizes: Optional[List]
