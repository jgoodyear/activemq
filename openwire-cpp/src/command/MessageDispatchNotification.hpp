/*
* Copyright 2006 The Apache Software Foundation or its licensors, as
* applicable.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#ifndef MessageDispatchNotification_hpp_
#define MessageDispatchNotification_hpp_

#include <string>

/* we could cut this down  - for now include all possible headers */
#include "command/BaseCommand.hpp"
#include "command/BrokerId.hpp"
#include "command/ConnectionId.hpp"
#include "command/ConsumerId.hpp"
#include "command/ProducerId.hpp"
#include "command/SessionId.hpp"

#include "command/BaseCommand.hpp"
#include "util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace command
      {
        using namespace ifr;
        using namespace apache::activemq::client;

/*
 *
 *  Marshalling code for Open Wire Format for MessageDispatchNotification
 *
 *
 *  NOTE!: This file is autogenerated - do not modify!
 *         if you need to make a change, please see the Groovy scripts in the
 *         activemq-core module
 *
 */
class MessageDispatchNotification : public BaseCommand
{
private:
    p<ConsumerId> consumerId ;
    ActiveMQDestination destination ;
    long deliverySequenceId ;
    p<MessageId> messageId ;

public:
    const static int TYPE = 90;

public:
    MessageDispatchNotification() ;
    virtual ~MessageDispatchNotification() ;


    virtual p<ConsumerId> getConsumerId() ;
    virtual void setConsumerId(p<ConsumerId> consumerId) ;

    virtual ActiveMQDestination getDestination() ;
    virtual void setDestination(ActiveMQDestination destination) ;

    virtual long getDeliverySequenceId() ;
    virtual void setDeliverySequenceId(long deliverySequenceId) ;

    virtual p<MessageId> getMessageId() ;
    virtual void setMessageId(p<MessageId> messageId) ;


} ;

/* namespace */
      }
    }
  }
}

#endif /*MessageDispatchNotification_hpp_*/
