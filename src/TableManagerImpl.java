import java.util.*;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.*;
import com.apple.foundationdb.tuple.Tuple;
import org.w3c.dom.Attr;


/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{

  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType,
                         String[] primaryKeyAttributeNames) {
    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;
    DirectorySubspace tableDir = null;



    if (attributeNames == null || attributeNames.length == 0) {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }

    if (attributeType == null || attributeType.length != attributeNames.length) {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }
    if (primaryKeyAttributeNames == null || primaryKeyAttributeNames.length == 0) {
      return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
    }
    for (String pkan : Arrays.asList(primaryKeyAttributeNames)) {
      if (!Arrays.asList(attributeNames).contains(pkan)) {
        return StatusCode.TABLE_CREATION_PRIMARY_KEY_NOT_FOUND;
      }
    }

    for (int i = 0; i < attributeNames.length; ++i) {
      for (int j = i+1; j < attributeNames.length; ++j) {
        if (attributeNames[i].equals(attributeNames[j]))
          return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
      }
    }

    for (AttributeType at : attributeType) {
      if (!at.equals(AttributeType.INT) && !at.equals(AttributeType.VARCHAR) && !at.equals(AttributeType.DOUBLE)) {
        return StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED;
      }
    }

    try {
      db = fdb.open();

      List<String> dirs = DirectoryLayer.getDefault().list(db).join();
      for (String d : dirs) {
        if (d.equals(tableName)) {
          return StatusCode.TABLE_ALREADY_EXISTS;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e);
    }

    TableMetadata tm = new TableMetadata(attributeNames,attributeType,primaryKeyAttributeNames);
    try {
      Transaction tx = db.createTransaction();
      tableDir = DirectoryLayer.getDefault().create(db,PathUtil.from(tableName)).join();

//      System.out.println("Path (in create) " + tableDir.getPath());


      tmToDB(db,tx,tableDir, tm);
      tx.commit().join();
      tx.close();

    } catch (DirectoryAlreadyExistsException e) {
      return StatusCode.TABLE_ALREADY_EXISTS;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e);
    } finally {
      if (db != null)
        db.close();
    }

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    // your code
    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;

   try {
     db = fdb.open();
     List<String> tables = DirectoryLayer.getDefault().list(db).join();
     if (!tables.contains(tableName)) {
       db.close();
       return StatusCode.TABLE_NOT_FOUND;
     }

   } catch (Exception e) {
     e.printStackTrace();
     System.out.println(e);
   }

    try {
      DirectoryLayer.getDefault().remove(db,PathUtil.from(tableName)).join();

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e);
    } finally {
      if (db != null) db.close();
    }
    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    // your code
    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;
    HashMap<String,TableMetadata> out = new HashMap<>();


    try {
      db = fdb.open();
      List<String> dirs = DirectoryLayer.getDefault().list(db).join();
      Transaction tx = null;
      for (String d : dirs) {
        tx = db.createTransaction();
//        System.out.println("dir: " + d);
        DirectorySubspace tabledir = DirectoryLayer.getDefault().open(tx,PathUtil.from(d)).join();

        TableMetadata tm = tmFromDB(db, tx, tabledir);

        out.put(d,tm);
        tx.close();

      }
//      if (tx != null) {
//        tx.commit().join();
//        tx.close();
//      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e);
    } finally {
      if (db != null)
        db.close();
    }
    return out;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {

    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;

    try {

      db = fdb.open();

      List<String> dirs = DirectoryLayer.getDefault().list(db).join();
      if (!dirs.contains(tableName)) {
        db.close();
        return StatusCode.TABLE_NOT_FOUND;
      }
      DirectorySubspace tableDir = DirectoryLayer.getDefault().open(db,PathUtil.from(tableName)).join();

      Transaction tx = db.createTransaction();
      TableMetadata tm = tmFromDB(db,tx,tableDir);

      if (tm.getAttributes().containsKey(attributeName)) {
        tx.commit().join();
        tx.close();
        db.close();
        return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
      }

      if (attributeType != AttributeType.INT && attributeType != AttributeType.DOUBLE && attributeType != AttributeType.VARCHAR) {
        tx.commit().join();
        tx.close();
        db.close();
        return StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED;
      }

      tm.addAttribute(attributeName,attributeType);

      tmToDB(db, tx,tableDir,tm);
      tx.commit().join();
      tx.close();
      return StatusCode.SUCCESS;
    } catch (NoSuchDirectoryException e) {
      return StatusCode.TABLE_NOT_FOUND;
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (db != null)
        db.close();
    }
    return null;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {

    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;

    try {
      db = fdb.open();
      Transaction tx = db.createTransaction();
      DirectorySubspace tableDir = DirectoryLayer.getDefault().open(tx, PathUtil.from(tableName)).join();

      TableMetadata tm = tmFromDB(db,tx,tableDir);

      if (!tm.getAttributes().containsKey(attributeName)) {
        tx.close();
        db.close();
        return StatusCode.ATTRIBUTE_NOT_FOUND;
      }

      HashMap<String, AttributeType> hm = tm.getAttributes();

      hm.remove(attributeName);

      tm.setAttributes(hm);

      tmToDB(db, tx,tableDir,tm);
      tx.commit().join();
      tx.close();

    } catch (NoSuchDirectoryException e) {
      return StatusCode.TABLE_NOT_FOUND;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e);
    } finally {
      if (db != null)
        db.close();
    }



    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;

    try {
      db = fdb.open();
      List<String> dirs = DirectoryLayer.getDefault().list(db).join();
      Transaction tx = db.createTransaction();
      for (String d : dirs) {
        DirectoryLayer.getDefault().remove(tx,PathUtil.from(d)).join();
      }
      tx.commit().join();
      tx.close();

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e);
    } finally {
      if (db != null)
        db.close();
    }
    return StatusCode.SUCCESS;
  }

  private TableMetadata tmFromDB(Database db, Transaction tx, DirectorySubspace tabledir) throws Exception {
      DirectorySubspace metadir = DirectoryLayer.getDefault().open(tx, PathUtil.extend(tabledir.getPath(), "metadata")).join();

      Tuple primKeysKeyTup = new Tuple();
      primKeysKeyTup = primKeysKeyTup.add("primaryKeys");
      byte[] key = metadir.pack(primKeysKeyTup);
      byte[] gottenBytes = tx.get(key).get();
      Tuple primKeysTup = Tuple.fromBytes(gottenBytes);
      List<Object> primKeysList = primKeysTup.getItems();

      Tuple attrsKeyTup = new Tuple();
      attrsKeyTup = attrsKeyTup.add("attrs");
      Tuple attrsTup = Tuple.fromBytes(tx.get(metadir.pack(attrsKeyTup)).get());
      List<Object> attrsList = attrsTup.getItems();
      HashMap<String, AttributeType> hm = new HashMap<>();

//      tx.commit().join();
      for (Object o : attrsList) {
        Transaction tr = db.createTransaction();
        Tuple keyTup = new Tuple();
        String attrName = (String) o;
//        System.out.println("Object: " + o);
        keyTup = keyTup.add("attrType").add(attrName);

        Tuple val = Tuple.fromBytes(tr.get(metadir.pack(keyTup)).get());
        tr.commit().join();
        if (val.getString(0).equals("INT")) {
          hm.put(attrName, AttributeType.INT);
        } else if (val.getString(0).equals("VARCHAR")) {
          hm.put(attrName, AttributeType.VARCHAR);
        } else if (val.getString(0).equals("DOUBLE")) {
          hm.put(attrName, AttributeType.DOUBLE);
        }
        tr.close();
      }

    List<String> primKeysOut = new ArrayList<>();
    for (Object o : primKeysList) {
      primKeysOut.add((String) o);
    }

    List<String> attrListOut = new ArrayList<>();
    for (Object o : attrsList) {
      attrListOut.add((String) o);
    }
    List<AttributeType> attrTListOut = new ArrayList<>();
    for (String s : attrListOut) {
      attrTListOut.add(hm.get(s));
    }
    TableMetadata tm = new TableMetadata(attrListOut.toArray(new String[0]),attrTListOut.toArray(new AttributeType[0]),primKeysOut.toArray(new String[0]));

    return tm;
  }


  private void tmToDB(Database db,Transaction tx, DirectorySubspace tableDir, TableMetadata tm) {
    DirectorySubspace metadir = DirectoryLayer.getDefault().createOrOpen(db,PathUtil.extend(tableDir.getPath(),"metadata")).join();
    Tuple primaryKeysKeyTup = new Tuple();
    primaryKeysKeyTup = primaryKeysKeyTup.add("primaryKeys");
    List<String> primaryKeyAttributeNames = tm.getPrimaryKeys();
    Tuple primaryKeysValTup = Tuple.fromList(primaryKeyAttributeNames);
    tx.set(metadir.pack(primaryKeysKeyTup),primaryKeysValTup.pack());
    List<String> attributeNames = new ArrayList<>();
    attributeNames.addAll(tm.getAttributes().keySet());
    Tuple AttrsKeyTup = new Tuple();
    AttrsKeyTup = AttrsKeyTup.add("attrs");
    Tuple AttrsValTup = Tuple.fromList(attributeNames);
    tx.set(metadir.pack(AttrsKeyTup),AttrsValTup.pack());

    HashMap<String, AttributeType> attrs = tm.getAttributes();

    for (Map.Entry<String, AttributeType> attr : attrs.entrySet()) {
      Tuple keyTup = new Tuple();
      keyTup = keyTup.add("attrType").add(attr.getKey());
      Tuple valTup = new Tuple();
//      System.out.println(attr.getValue());
      valTup = valTup.add(attr.getValue().toString());
      tx.set(metadir.pack(keyTup),valTup.pack());
    }

  }
}
