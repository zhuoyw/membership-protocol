/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode() == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode() {
	/*
	 * This function is partially implemented and may require changes
	 */
	//int id = *(int*)(&memberNode->addr.addr);
	//int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg)+sizeof(MessageHdr), memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg)+sizeof(MessageHdr)+ sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif
        fprintf(stderr, "before send req\n");
        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();


    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    size_t offset = 0;
    MessageHdr* msg = (MessageHdr*) malloc(sizeof(char) * size);
    memcpy(msg, data, sizeof(char) * size);
    offset += sizeof(MessageHdr);

    switch (msg->msgType)
    {
        case JOINREQ:
        {
            //for the introducer
            Address addr;
            long heartbeat;
            memcpy(addr.addr, (char*)(msg)+offset, sizeof(addr.addr));
            memcpy(&heartbeat, (char*)(msg)+sizeof(MessageHdr)+sizeof(addr.addr), sizeof(long));
            //add to membership list
            addMemberEntry(getid(&addr), getport(&addr), heartbeat);
            //send JOINREP
            //printAddress(&addr);
            sendMemberList(&addr, JOINREP);
            break;
        }
        case JOINREP:
        {
            memberNode->inGroup = true;

            int count;
            memcpy(&count, (char*)(msg)+offset, sizeof(int));
            offset += sizeof(int);

            for (int i = 0; i < count; ++i)
            {
                int id;
                memcpy(&id, (char*)(msg)+offset, sizeof(int));
                offset += sizeof(int);

                short port;
                memcpy(&port, (char*)(msg)+offset, sizeof(short));
                offset += sizeof(short);

                long heartbeat;
                memcpy(&heartbeat, (char*)(msg)+offset, sizeof(long));
                offset += sizeof(long);

                addMemberEntry(id, port, heartbeat);
            }

            break;
        }
        case GOSSIP:
        {

            //printAddress(&memberNode->addr);

            int count;
            memcpy(&count, (char*)(msg)+offset, sizeof(int));
            offset += sizeof(int);

            //printf("count%d\n", count);
            for (int i = 0; i < count; ++i)
            {
                int id;
                memcpy(&id, (char*)(msg)+offset, sizeof(int));
                offset += sizeof(int);

                short port;
                memcpy(&port, (char*)(msg)+offset, sizeof(short));
                offset += sizeof(short);

                long heartbeat;
                memcpy(&heartbeat, (char*)(msg)+offset, sizeof(long));
                offset += sizeof(long);

                updateMemberEntry(id, port, heartbeat);
            }
            break;
        }
        default:
            return false;
    }
    free(msg);
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    vector<MemberListEntry>::iterator it;
    //printAddress(&memberNode->addr);
    //printf("%d\n", memberNode->pingCounter);
    memberNode->pingCounter -= 1;
    if (0 == memberNode->pingCounter)
    {
        memberNode->heartbeat += 1;
        for (it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
            if (memberNode->timeOutCounter != it->timestamp)
            {
                Address addr;
                makeAddress(&addr, it->id, it->port);
                sendMemberList(&addr, GOSSIP);
            }
        }
        memberNode->pingCounter = TFAIL;
        //printAddress(&memberNode->addr);
    }

    memberNode->timeOutCounter += 1;
    for (it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ) {
        if (memberNode->timeOutCounter - TREMOVE > it->timestamp) {
            Address addr;
            makeAddress(&addr, it->id, it->port);
            #ifdef DEBUGLOG
            log->logNodeRemove(&memberNode->addr, &addr);
            #endif
            it = memberNode->memberList.erase(it);
        }
        else {
            it++;
        }
    }
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: addMemberEntry
 *
 * DESCRIPTION: add new entry to the membership list
 */
void MP1Node::addMemberEntry(int id, short port, long heartbeat) {
    Address addr;
    makeAddress(&addr, id, port);
    if (!findMemberEntry(id, port))
    {
        #ifdef DEBUGLOG
        log->logNodeAdd(&memberNode->addr, &addr);
        #endif
        MemberListEntry entry = MemberListEntry(id, port, heartbeat, memberNode->timeOutCounter);
        memberNode->memberList.push_back(entry);
    }
    return;
}

/**
 * FUNCTION NAME: findMemberEntry
 *
 * DESCRIPTION: add new entry to the membership list
 */
bool MP1Node::findMemberEntry(int id, short port) {
    vector<MemberListEntry>::iterator it;
    for (it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it)
    {
        if (id == it->id && port == it->port)
            return true;
    }
    return false;
}

void MP1Node::updateMemberEntry(int id, short port, long heartbeat) {
    vector<MemberListEntry>::iterator it;
    for (it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it)
    {
        if (id == it->id && port == it->port)
        {
            if (heartbeat > it->heartbeat)
            {
                it->heartbeat = heartbeat;
                it->timestamp = memberNode->timeOutCounter;
            }
            return;
        }
    }
    addMemberEntry(id, port, heartbeat);
    return;
}

void MP1Node::sendMemberList(Address* addr, MsgTypes msgtype)
{
    size_t msgsize = sizeof(MessageHdr)
                        + sizeof(int)
                        + (memberNode->memberList.size() + 1) * (sizeof(int) + sizeof(short) + sizeof(long));

    MessageHdr* msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    size_t offset = 0;

    msg->msgType = msgtype;
    offset += sizeof(MessageHdr);

    int count = 1;//memberNode->memberList.size() + 1;
    //memcpy((char*)(msg)+sizeof(MessageHdr), &count, sizeof(int));
    offset += sizeof(int);

    int id = getid(&memberNode->addr);
    memcpy((char*)(msg)+offset, &id, sizeof(int));
    offset += sizeof(int);

    short port = getport(&memberNode->addr);
    memcpy((char*)(msg)+offset, &port, sizeof(short));
    offset += sizeof(short);

    memcpy((char*)(msg)+offset, &memberNode->heartbeat, sizeof(long));
    offset += sizeof(long);

    vector<MemberListEntry>::iterator it;
    for (it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
        if (memberNode->timeOutCounter - TFAIL <= it->timestamp)
        {
            count += 1;

            memcpy((char*)(msg)+offset, &it->id, sizeof(int));
            offset += sizeof(int);

            memcpy((char*)(msg)+offset, &it->port, sizeof(short));
            offset += sizeof(short);

            memcpy((char*)(msg)+offset, &it->heartbeat, sizeof(long));
            offset += sizeof(long);
        }
    }

    memcpy((char*)(msg)+sizeof(MessageHdr), &count, sizeof(int));

    msgsize = offset;

    emulNet->ENsend(&memberNode->addr, addr, (char *)msg, msgsize);

    return;
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}

int MP1Node::getid(Address* addr) {
    return *(int*)(&addr->addr[0]);
}

short MP1Node::getport(Address* addr) {
    return *(short*)(&addr->addr[4]);
}

void MP1Node::makeAddress(Address* addr, int id, short port) {
    *(int *)(&addr->addr[0]) = id;
    *(short *)(&addr->addr[4]) = port;
    return;
}