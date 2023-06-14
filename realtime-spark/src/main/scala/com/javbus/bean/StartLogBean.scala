package com.javbus.bean

case class StartLogBean(
                         province_id: String,
                         brand: String,
                         channel: String,
                         is_new: String,
                         model: String,
                         mid: String,
                         operate_system: String,
                         user_id: String,
                         version_code: String,

                         entry: String,
                         loading_time: Long,
                         open_ad_id: String,
                         open_ad_ms: Long,
                         open_ad_skip_ms: Long,
                         ts: Long)
