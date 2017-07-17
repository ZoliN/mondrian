/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.cache;

import mondrian.spi.*;
import mondrian.util.ByteString;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.FileUtils;



/**
 * Implementation of {@link mondrian.spi.SegmentCache} that stores segments
 * on disk.
 *
 * <p>Not thread safe.</p>
 *
 * @author Zoltán Nagy
 */
public class DiskSegmentCache implements SegmentCache {
    private String cacheDir = System.getProperty("java.io.tmpdir") + File.separator + "mondriandc" + File.separator;
    
    private final Set<ByteString> cachedIDs =
        new HashSet<ByteString>();

    private final List<SegmentCacheListener> listeners =
        new CopyOnWriteArrayList<SegmentCacheListener>();

    public DiskSegmentCache() {
        FileUtils.deleteQuietly(new File(cacheDir));
        try {
            FileUtils.forceMkdir(new File(cacheDir));
        } catch (IOException e) {

        }
    }
    
    public SegmentBody get(SegmentHeader header) {
        System.out.println("disk sc get:" + header.hashCode());
        if (!cachedIDs.contains(header.getUniqueID())) return null;

        FileInputStream fileIn = null;
        ObjectInputStream in = null;
        SegmentBody body = null;
        try {
            String path = cacheDir + header.schemaName + File.separator  + header.cubeName + File.separator + header.rolapStarFactTableName + File.separator + header.constrainedColsBitKey.toString();
            fileIn = new FileInputStream(path + File.separator + header.getUniqueID());
            in = new ObjectInputStream(fileIn);
            body = (SegmentBody) in.readObject();
        } catch(FileNotFoundException i) {
            cachedIDs.remove(header.getUniqueID());
        } catch(IOException i) {
        } catch(ClassNotFoundException c) {
        } finally {
            if (in != null)
                try {
                    in.close();
                } catch (IOException e) {
                }
            if (fileIn != null)
                try {
                    fileIn.close();
                } catch (IOException e) {
                }
        }
        if (body == null) System.out.println("disk sc get: body null");
        return body;

    }

    public boolean contains(SegmentHeader header) {
        System.out.println("disk sc contains:" + header.hashCode());
        if (!cachedIDs.contains(header.getUniqueID())) return false;

        FileInputStream fileIn = null;
        ObjectInputStream in = null;
        try {
            String path = cacheDir + header.schemaName + File.separator  + header.cubeName + File.separator + header.rolapStarFactTableName + File.separator + header.constrainedColsBitKey.toString();
            fileIn = new FileInputStream(path + File.separator + header.getUniqueID());
        } catch(FileNotFoundException i) {
            remove(header);
            return false;
        } finally {
            if (in != null)
                try {
                    in.close();
                } catch (IOException e) {
                }
            if (fileIn != null)
                try {
                    fileIn.close();
                } catch (IOException e) {
                }
        }
        return true;
    }

    public List<SegmentHeader> getSegmentHeaders() {
        return Collections.emptyList();
    }

    public boolean put(final SegmentHeader header, SegmentBody body) {
        // REVIEW: What's the difference between returning false
        // and throwing an exception?
        System.out.println("disk sc put:" + header.hashCode());
        
        FileOutputStream fileOut = null;
        ObjectOutputStream out = null;
        try {
            String path = cacheDir + header.schemaName + File.separator  + header.cubeName + File.separator + header.rolapStarFactTableName + File.separator + header.constrainedColsBitKey.toString();
            FileUtils.forceMkdir(new File(path));
            fileOut = new FileOutputStream(path + File.separator + header.getUniqueID());
            out = new ObjectOutputStream(fileOut);
            out.writeObject(body);
        } catch(IOException i) {
            return false;
        } finally {
            if (out != null)
                try {
                    out.close();
                } catch (IOException e) {
                }
            if (fileOut != null)
                try {
                    fileOut.close();
                } catch (IOException e) {
                }
        }

        cachedIDs.add(header.getUniqueID());
        fireSegmentCacheEvent(
            new SegmentCache.SegmentCacheListener.SegmentCacheEvent() {
                public boolean isLocal() {
                    return true;
                }
                public SegmentHeader getSource() {
                    return header;
                }
                public EventType getEventType() {
                    return SegmentCacheListener.SegmentCacheEvent
                        .EventType.ENTRY_CREATED;
                }
            });
        return true; // success
    }

    public boolean remove(final SegmentHeader header) {
        System.out.println("disk sc remove:" + header.hashCode());
        final boolean result =
                cachedIDs.remove(header.getUniqueID());
        if (result) {
            fireSegmentCacheEvent(
                new SegmentCache.SegmentCacheListener.SegmentCacheEvent() {
                    public boolean isLocal() {
                        return true;
                    }
                    public SegmentHeader getSource() {
                        return header;
                    }
                    public EventType getEventType() {
                        return
                            SegmentCacheListener.SegmentCacheEvent
                                .EventType.ENTRY_DELETED;
                    }
                });
        }
        return result;
    }

    public void tearDown() {
        cachedIDs.clear();
        listeners.clear();
        FileUtils.deleteQuietly(new File(cacheDir));
    }

    public void addListener(SegmentCacheListener listener) {
        listeners.add(listener);
    }

    public void removeListener(SegmentCacheListener listener) {
        listeners.remove(listener);
    }

    public boolean supportsRichIndex() {
        return false;
    }

    public void fireSegmentCacheEvent(
        SegmentCache.SegmentCacheListener.SegmentCacheEvent evt)
    {
        for (SegmentCacheListener listener : listeners) {
            listener.handle(evt);
        }
    }
}

// End MemorySegmentCache.java
