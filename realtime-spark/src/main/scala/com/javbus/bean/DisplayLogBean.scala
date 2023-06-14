package com.javbus.bean

case class DisplayLogBean (
                      province_id: String,
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

                      display_type: String,
                      display_item: String,
                      display_item_type: String,
                      display_order: String,
                      display_pos_id: String,

                      ts: Long
)