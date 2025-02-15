# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：  
# 1. 不得用于任何商业用途。  
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。  
# 3. 不得进行大规模爬取或对平台造成运营干扰。  
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。   
# 5. 不得用于任何非法或不当的用途。
#   
# 详细许可条款请参阅项目根目录下的LICENSE文件。  
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。  


# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2024/1/14 17:34
# @Desc    :
import datetime
from typing import List

import config
from var import source_keyword_var

from . import xhs_store_impl
from .xhs_store_image import *
from .xhs_store_impl import *


class XhsStoreFactory:
    STORES = {
        "csv": XhsCsvStoreImplement,
        "db": XhsDbStoreImplement,
        "json": XhsJsonStoreImplement
    }

    @staticmethod
    def create_store() -> AbstractStore:
        store_class = XhsStoreFactory.STORES.get(config.SAVE_DATA_OPTION)
        if not store_class:
            raise ValueError("[XhsStoreFactory.create_store] Invalid save option only supported csv or db or json ...")
        return store_class()

redis_mine = MyRedisCache()

def get_video_url_arr(note_item: Dict) -> List:
    """
    获取视频url数组
    Args:
        note_item:

    Returns:

    """
    if note_item.get('type') != 'video':
        return []

    videoArr = []
    originVideoKey = note_item.get('video').get('consumer').get('origin_video_key')
    if originVideoKey == '':
        originVideoKey = note_item.get('video').get('consumer').get('originVideoKey')
    # 降级有水印
    if originVideoKey == '':
        videos = note_item.get('video').get('media').get('stream').get('h264')
        if type(videos).__name__ == 'list':
            videoArr = [v.get('master_url') for v in videos]
    else:
        videoArr = [f"http://sns-video-bd.xhscdn.com/{originVideoKey}"]

    return videoArr


async def update_xhs_note(note_item: Dict):
    """
    更新小红书笔记
    Args:
        note_item:

    Returns:

    """
    note_id = note_item.get("note_id")

    ## if note already in redis_mine, store its content directly
    if redis_mine.is_exists(f"xhs_note:{note_id}"):
        utils.logger.info(f"[store.xhs.update_xhs_note] {note_id} already existing, store content directly")
        await XhsStoreFactory.create_store().store_content(note_item)
        return

    
    user_info = note_item.get("user", {})
    interact_info = note_item.get("interact_info", {})
    # image_list: List[Dict] = note_item.get("image_list", [])
    tag_list: List[Dict] = note_item.get("tag_list", [])

    # for img in image_list:
    #     if img.get('url_default') != '':
    #         img.update({'url': img.get('url_default')})

    # video_url = ','.join(get_video_url_arr(note_item))

    local_db_item = {
        "source_keyword": source_keyword_var.get(),
        "note_id": note_item.get("note_id"),
        "type": note_item.get("type"),
        "title": note_item.get("title") or note_item.get("desc", "")[:255],
        "desc": note_item.get("desc", ""),
        # "video_url": video_url,
        "time": note_item.get("time"),
        "last_update_time": note_item.get("last_update_time", 0),
        # convert milliseconds time and last_update_time to yyyy-mm-dd hh:mm:ss string format
        "time_readable": datetime.datetime.fromtimestamp(note_item.get("time") / 1000.0).strftime('%Y-%m-%d %H:%M:%S'),
        "last_update_time_readable": datetime.datetime.fromtimestamp(note_item.get("last_update_time") / 1000.0).strftime('%Y-%m-%d %H:%M:%S'),


        "user_id": user_info.get("user_id"),
        "nickname": user_info.get("nickname"),
        # "avatar": user_info.get("avatar"),
        "liked_count": interact_info.get("liked_count"),
        "collected_count": interact_info.get("collected_count"),
        "comment_count": interact_info.get("comment_count"),
        "share_count": interact_info.get("share_count"),
        "ip_location": note_item.get("ip_location", ""),
        # "image_list": ','.join([img.get('url', '') for img in image_list]),
        "tag_list": ','.join([tag.get('name', '') for tag in tag_list if tag.get('type') == 'topic']),
        # get current date
        "crawl_date": datetime.datetime.now().strftime('%Y-%m-%d'),
        "note_url": f"https://www.xiaohongshu.com/explore/{note_id}?xsec_token={note_item.get('xsec_token')}&xsec_source=pc_search",
        
    }
    # utils.logger.info(f"[store.xhs.update_xhs_note] xhs note: {local_db_item}")
    await XhsStoreFactory.create_store().store_content(local_db_item)


async def batch_update_xhs_note_comments(note_id: str, comments: List[Dict]):
    """
    批量更新小红书笔记评论
    Args:
        note_id:
        comments:

    Returns:

    """
    if not comments:
        return
    for comment_item in comments:
        await update_xhs_note_comment(note_id, comment_item)


async def update_xhs_note_comment(note_id: str, comment_item: Dict):
    """
    更新小红书笔记评论
    Args:
        note_id:
        comment_item:

    Returns:

    """
    user_info = comment_item.get("user_info", {})
    comment_id = comment_item.get("id")
    comment_pictures = [item.get("url_default", "") for item in comment_item.get("pictures", [])]
    target_comment = comment_item.get("target_comment", {})
    local_db_item = {
        "comment_id": comment_id,
        "create_time": comment_item.get("create_time"),
        "ip_location": comment_item.get("ip_location"),
        "note_id": note_id,
        "content": comment_item.get("content"),
        "user_id": user_info.get("user_id"),
        "nickname": user_info.get("nickname"),
        "avatar": user_info.get("image"),
        "sub_comment_count": comment_item.get("sub_comment_count", 0),
        "pictures": ",".join(comment_pictures),
        "parent_comment_id": target_comment.get("id", 0),
        "last_modify_ts": utils.get_current_timestamp(),
        "like_count": comment_item.get("like_count", 0),
    }
    utils.logger.info(f"[store.xhs.update_xhs_note_comment] xhs note comment:{local_db_item}")
    await XhsStoreFactory.create_store().store_comment(local_db_item)


async def save_creator(user_id: str, creator: Dict):
    """
    保存小红书创作者
    Args:
        user_id:
        creator:

    Returns:

    """
    user_info = creator.get('basicInfo', {})

    follows = 0
    fans = 0
    interaction = 0
    for i in creator.get('interactions'):
        if i.get('type') == 'follows':
            follows = i.get('count')
        elif i.get('type') == 'fans':
            fans = i.get('count')
        elif i.get('type') == 'interaction':
            interaction = i.get('count')

    local_db_item = {
        'user_id': user_id,
        'nickname': user_info.get('nickname'),
        'gender': '女' if user_info.get('gender') == 1 else '男',
        'avatar': user_info.get('images'),
        'desc': user_info.get('desc'),
        'ip_location': user_info.get('ipLocation'),
        'follows': follows,
        'fans': fans,
        'interaction': interaction,
        'tag_list': json.dumps({tag.get('tagType'): tag.get('name') for tag in creator.get('tags')},
                               ensure_ascii=False),
        "last_modify_ts": utils.get_current_timestamp(),
    }
    utils.logger.info(f"[store.xhs.save_creator] creator:{local_db_item}")
    await XhsStoreFactory.create_store().store_creator(local_db_item)


async def update_xhs_note_image(note_id, pic_content, extension_file_name):
    """
    更新小红书笔
    Args:
        note_id:
        pic_content:
        extension_file_name:

    Returns:

    """

    await XiaoHongShuImage().store_image(
        {"notice_id": note_id, "pic_content": pic_content, "extension_file_name": extension_file_name})
