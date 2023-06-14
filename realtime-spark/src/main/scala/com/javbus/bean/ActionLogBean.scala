package com.javbus.bean

case class ActionLogBean(province_id: String,
                    brand: String,
                    channel: String,
                    is_new: String,
                    model: String,
                    mid: String,
                    operate_system: String,
                    user_id: String,
                    version_code: String,

                    during_time: Long,
                    page_item: String,
                    page_item_type: String,
                    last_page_id: String,
                    page_id: String,
                    source_type: String,

                    action_id: String,
                    action_item: String,
                    action_item_type: String,

                    ts: Long
)
