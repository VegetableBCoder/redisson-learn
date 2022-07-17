/**
 * Copyright (c) 2013-2021 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.api.listener;

/**
 * Base status listener for Redis PubSub channel status changes
 *
 * @author Nikita Koksharov
 *
 * @see org.redisson.api.RTopic
 */
public class BasePatternStatusListener implements PatternStatusListener {

    //空的 只在单元测试中用到了这个类 使用的时候自己去继承扩展(提供接口的默认实现)
    @Override
    public void onPSubscribe(String channel) {
    }

    @Override
    public void onPUnsubscribe(String channel) {
    }

}
