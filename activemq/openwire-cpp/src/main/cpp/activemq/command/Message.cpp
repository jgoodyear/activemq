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
#include "activemq/command/Message.hpp"

using namespace apache::activemq::command;

/*
 *
 *  Marshalling code for Open Wire Format for Message
 *
 *
 *  NOTE!: This file is autogenerated - do not modify!
 *         if you need to make a change, please see the Groovy scripts in the
 *         activemq-core module
 *
 */
Message::Message()
{
    this->producerId = NULL ;
    this->destination = NULL ;
    this->transactionId = NULL ;
    this->originalDestination = NULL ;
    this->messageId = NULL ;
    this->originalTransactionId = NULL ;
    this->groupID = NULL ;
    this->groupSequence = 0 ;
    this->correlationId = NULL ;
    this->persistent = false ;
    this->expiration = 0 ;
    this->priority = 0 ;
    this->replyTo = NULL ;
    this->timestamp = 0 ;
    this->type = NULL ;
    this->content = NULL ;
    this->marshalledProperties = NULL ;
    this->dataStructure = NULL ;
    this->targetConsumerId = NULL ;
    this->compressed = false ;
    this->redeliveryCounter = 0 ;
    this->brokerPath = NULL ;
    this->arrival = 0 ;
    this->userID = NULL ;
    this->recievedByDFBridge = false ;
}

Message::~Message()
{
}

unsigned char Message::getDataStructureType()
{
    return Message::TYPE ; 
}

        
p<ProducerId> Message::getProducerId()
{
    return producerId ;
}

void Message::setProducerId(p<ProducerId> producerId)
{
    this->producerId = producerId ;
}

        
p<ActiveMQDestination> Message::getDestination()
{
    return destination ;
}

void Message::setDestination(p<ActiveMQDestination> destination)
{
    this->destination = destination ;
}

        
p<TransactionId> Message::getTransactionId()
{
    return transactionId ;
}

void Message::setTransactionId(p<TransactionId> transactionId)
{
    this->transactionId = transactionId ;
}

        
p<ActiveMQDestination> Message::getOriginalDestination()
{
    return originalDestination ;
}

void Message::setOriginalDestination(p<ActiveMQDestination> originalDestination)
{
    this->originalDestination = originalDestination ;
}

        
p<MessageId> Message::getMessageId()
{
    return messageId ;
}

void Message::setMessageId(p<MessageId> messageId)
{
    this->messageId = messageId ;
}

        
p<TransactionId> Message::getOriginalTransactionId()
{
    return originalTransactionId ;
}

void Message::setOriginalTransactionId(p<TransactionId> originalTransactionId)
{
    this->originalTransactionId = originalTransactionId ;
}

        
p<string> Message::getGroupID()
{
    return groupID ;
}

void Message::setGroupID(p<string> groupID)
{
    this->groupID = groupID ;
}

        
int Message::getGroupSequence()
{
    return groupSequence ;
}

void Message::setGroupSequence(int groupSequence)
{
    this->groupSequence = groupSequence ;
}

        
p<string> Message::getCorrelationId()
{
    return correlationId ;
}

void Message::setCorrelationId(p<string> correlationId)
{
    this->correlationId = correlationId ;
}

        
bool Message::getPersistent()
{
    return persistent ;
}

void Message::setPersistent(bool persistent)
{
    this->persistent = persistent ;
}

        
long long Message::getExpiration()
{
    return expiration ;
}

void Message::setExpiration(long long expiration)
{
    this->expiration = expiration ;
}

        
char Message::getPriority()
{
    return priority ;
}

void Message::setPriority(char priority)
{
    this->priority = priority ;
}

        
p<ActiveMQDestination> Message::getReplyTo()
{
    return replyTo ;
}

void Message::setReplyTo(p<ActiveMQDestination> replyTo)
{
    this->replyTo = replyTo ;
}

        
long long Message::getTimestamp()
{
    return timestamp ;
}

void Message::setTimestamp(long long timestamp)
{
    this->timestamp = timestamp ;
}

        
p<string> Message::getType()
{
    return type ;
}

void Message::setType(p<string> type)
{
    this->type = type ;
}

        
array<char> Message::getContent()
{
    return content ;
}

void Message::setContent(array<char> content)
{
    this->content = content ;
}

        
array<char> Message::getMarshalledProperties()
{
    return marshalledProperties ;
}

void Message::setMarshalledProperties(array<char> marshalledProperties)
{
    this->marshalledProperties = marshalledProperties ;
}

        
p<IDataStructure> Message::getDataStructure()
{
    return dataStructure ;
}

void Message::setDataStructure(p<IDataStructure> dataStructure)
{
    this->dataStructure = dataStructure ;
}

        
p<ConsumerId> Message::getTargetConsumerId()
{
    return targetConsumerId ;
}

void Message::setTargetConsumerId(p<ConsumerId> targetConsumerId)
{
    this->targetConsumerId = targetConsumerId ;
}

        
bool Message::getCompressed()
{
    return compressed ;
}

void Message::setCompressed(bool compressed)
{
    this->compressed = compressed ;
}

        
int Message::getRedeliveryCounter()
{
    return redeliveryCounter ;
}

void Message::setRedeliveryCounter(int redeliveryCounter)
{
    this->redeliveryCounter = redeliveryCounter ;
}

        
array<BrokerId> Message::getBrokerPath()
{
    return brokerPath ;
}

void Message::setBrokerPath(array<BrokerId> brokerPath)
{
    this->brokerPath = brokerPath ;
}

        
long long Message::getArrival()
{
    return arrival ;
}

void Message::setArrival(long long arrival)
{
    this->arrival = arrival ;
}

        
p<string> Message::getUserID()
{
    return userID ;
}

void Message::setUserID(p<string> userID)
{
    this->userID = userID ;
}

        
bool Message::getRecievedByDFBridge()
{
    return recievedByDFBridge ;
}

void Message::setRecievedByDFBridge(bool recievedByDFBridge)
{
    this->recievedByDFBridge = recievedByDFBridge ;
}

int Message::marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> writer) throw (IOException)
{
    int size = 0 ;

    size += marshaller->marshalInt(commandId, mode, writer) ;
    size += marshaller->marshalBoolean(responseRequired, mode, writer) ; 
    size += marshaller->marshalObject(producerId, mode, writer) ; 
    size += marshaller->marshalObject(destination, mode, writer) ; 
    size += marshaller->marshalObject(transactionId, mode, writer) ; 
    size += marshaller->marshalObject(originalDestination, mode, writer) ; 
    size += marshaller->marshalObject(messageId, mode, writer) ; 
    size += marshaller->marshalObject(originalTransactionId, mode, writer) ; 
    size += marshaller->marshalString(groupID, mode, writer) ; 
    size += marshaller->marshalInt(groupSequence, mode, writer) ; 
    size += marshaller->marshalString(correlationId, mode, writer) ; 
    size += marshaller->marshalBoolean(persistent, mode, writer) ; 
    size += marshaller->marshalLong(expiration, mode, writer) ; 
    size += marshaller->marshalByte(priority, mode, writer) ; 
    size += marshaller->marshalObject(replyTo, mode, writer) ; 
    size += marshaller->marshalLong(timestamp, mode, writer) ; 
    size += marshaller->marshalString(type, mode, writer) ; 
    size += marshaller->marshalByteArray(content, mode, writer) ; 
    size += marshaller->marshalByteArray(marshalledProperties, mode, writer) ; 
    size += marshaller->marshalObject(dataStructure, mode, writer) ; 
    size += marshaller->marshalObject(targetConsumerId, mode, writer) ; 
    size += marshaller->marshalBoolean(compressed, mode, writer) ; 
    size += marshaller->marshalInt(redeliveryCounter, mode, writer) ; 
    size += marshaller->marshalObjectArray(brokerPath, mode, writer) ; 
    size += marshaller->marshalLong(arrival, mode, writer) ; 
    size += marshaller->marshalString(userID, mode, writer) ; 
    size += marshaller->marshalBoolean(recievedByDFBridge, mode, writer) ; 
    return size ;
}

void Message::unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> reader) throw (IOException)
{
    commandId = marshaller->unmarshalInt(mode, reader) ;
    responseRequired = marshaller->unmarshalBoolean(mode, reader) ; 
    producerId = p_cast<ProducerId>(marshaller->unmarshalObject(mode, reader)) ; 
    destination = p_cast<ActiveMQDestination>(marshaller->unmarshalObject(mode, reader)) ; 
    transactionId = p_cast<TransactionId>(marshaller->unmarshalObject(mode, reader)) ; 
    originalDestination = p_cast<ActiveMQDestination>(marshaller->unmarshalObject(mode, reader)) ; 
    messageId = p_cast<MessageId>(marshaller->unmarshalObject(mode, reader)) ; 
    originalTransactionId = p_cast<TransactionId>(marshaller->unmarshalObject(mode, reader)) ; 
    groupID = p_cast<string>(marshaller->unmarshalString(mode, reader)) ; 
    groupSequence = (marshaller->unmarshalInt(mode, reader)) ; 
    correlationId = p_cast<string>(marshaller->unmarshalString(mode, reader)) ; 
    persistent = (marshaller->unmarshalBoolean(mode, reader)) ; 
    expiration = (marshaller->unmarshalLong(mode, reader)) ; 
    priority = (marshaller->unmarshalByte(mode, reader)) ; 
    replyTo = p_cast<ActiveMQDestination>(marshaller->unmarshalObject(mode, reader)) ; 
    timestamp = (marshaller->unmarshalLong(mode, reader)) ; 
    type = p_cast<string>(marshaller->unmarshalString(mode, reader)) ; 
    content = (marshaller->unmarshalByteArray(mode, reader)) ; 
    marshalledProperties = (marshaller->unmarshalByteArray(mode, reader)) ; 
    dataStructure = p_cast<IDataStructure>(marshaller->unmarshalObject(mode, reader)) ; 
    targetConsumerId = p_cast<ConsumerId>(marshaller->unmarshalObject(mode, reader)) ; 
    compressed = (marshaller->unmarshalBoolean(mode, reader)) ; 
    redeliveryCounter = (marshaller->unmarshalInt(mode, reader)) ; 
    brokerPath = array_cast<BrokerId>(marshaller->unmarshalObjectArray(mode, reader)) ; 
    arrival = (marshaller->unmarshalLong(mode, reader)) ; 
    userID = p_cast<string>(marshaller->unmarshalString(mode, reader)) ; 
    recievedByDFBridge = (marshaller->unmarshalBoolean(mode, reader)) ; 
}
