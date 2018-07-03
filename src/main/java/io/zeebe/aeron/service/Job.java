package io.zeebe.aeron.service;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

import java.util.Date;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

public class Job {

  public static final int MSG_ID_OFFSET;
  public static final int CLIENT_SESSION_ID_OFFSET;
  public static final int LENGTH;

  static {
    int offset = 0;
    MSG_ID_OFFSET = offset;
    offset += SIZE_OF_INT;

    CLIENT_SESSION_ID_OFFSET = offset;
    offset += SIZE_OF_LONG;

    LENGTH = offset;
  }


  private final Date creationTime = new Date();


  private MessageIdentifier messageIdentifier = MessageIdentifier.JOB_CREATED;
  private long clientSessionId = 0;

  public Date getCreationTime() {
    return creationTime;
  }

  public MessageIdentifier getMessageIdentifier() {
    return messageIdentifier;
  }

  public void setMessageIdentifier(MessageIdentifier messageIdentifier) {
    this.messageIdentifier = messageIdentifier;
  }

  public long getClientSessionId() {
    return clientSessionId;
  }

  public void setClientSessionId(long clientSessionId) {
    this.clientSessionId = clientSessionId;
  }

  public DirectBuffer asBuffer()
  {
    final ExpandableArrayBuffer expandableArrayBuffer = new ExpandableArrayBuffer();

    expandableArrayBuffer.putInt(MSG_ID_OFFSET, messageIdentifier.ordinal());
    expandableArrayBuffer.putLong(CLIENT_SESSION_ID_OFFSET, clientSessionId);


    return expandableArrayBuffer;
  }

  public int length()
  {
    return LENGTH;
  }

  public void fromBuffer(DirectBuffer directBuffer, int offset, int length)
  {
    int msgId = directBuffer.getInt(MSG_ID_OFFSET + offset);
    messageIdentifier = MessageIdentifier.values()[msgId];
    clientSessionId = directBuffer.getLong(CLIENT_SESSION_ID_OFFSET + offset);
  }
}
