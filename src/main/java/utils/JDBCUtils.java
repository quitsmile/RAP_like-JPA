package utils;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * 一个功能上类似于JPA的小demo，基于约定大于编码的基础，默认所有属性与表中的列名都是一一对应的，
 * 否则无法使用。只支持一些简单的CRUD，连接从JDBC获取，支持事务。
 * {@code @author} R
 * @param <T> 当前类所对应的目标类类型
 * {@code @Version} 1.0
 */
public class JDBCUtils<T> {

    /**
     *
     * @param k 确定StringJoiner的类型
     * @param m 确定需要的是哪种SQL语句
     * @return 返回SQL语句中的主要部分，或者是一个Wrapper
     */

     public Object SQLHandler(K k,OPERATION m){

        Field[] fields = tClass.getDeclaredFields();

        if (fields.length==0){
            throw new RuntimeException("不能没有属性");
        }

        List<String> collect = Arrays.stream(fields).
                filter(a -> a.getDeclaringClass().equals(boolean.class)).
                map(field1 -> field1.getName().toLowerCase()).collect(Collectors.toList());

        Method[] methods = tClass.getDeclaredMethods();

        List<String> fieldsName = Arrays.stream(methods).
                filter(a -> a.getName().startsWith("set")).
                map(a -> a.getName().substring(3).toLowerCase()).
                collect(Collectors.toList());

        if (collect.size()>0){
            fieldsName.addAll(collect);
        }

        if (fieldsName.size()==0){
            throw new RuntimeException("不能没有属性");
        }

        if (!Optional.ofNullable(m).isPresent()){
            return new Wrapper(fields);
        }
        StringJoiner str = m.equals(OPERATION.INS)?stringJoinerFactory(K.DQL):stringJoinerFactory(k) ;

        switch (m){
            case INS:
            case SEL_ALL:
            case SEL:
                for (Field field : fields) {
                    if (fieldsName.contains(field.getName())) {

                        str.add(field.getName());
                    }
                }
                break;
            case UPD:
            case DEL:
                for (Field field : fields) {
                    if (!fieldsName.contains(field.getName()) || rootFiledName.equals(field.getName())) {
                        continue;
                    }
                    str.add(field.getName());
                }
                break;

        }

       return str +(m==OPERATION.UPD?" = ? ":EMPTY);
    }
    /**
     *
     * @param targets 要修改的目标对象的 List
     * @return 返回对应的执行情况，1是成功，0是失败，但如果失败会抛出异常的。
     */
    public List<Integer> updateAll(@NotNull List<T> targets){
         if (targets.size()==0)return null;
         ThreadPoolTransformerFlag = true;
        List<Integer> result = targets.stream().map(a -> (Callable<Integer>) () -> update(a)).map(fixed::submit).map(future -> {
            try {
                return future.get();
            } catch (InterruptedException | ExecutionException e) {
                rollbackAll();
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        commitAll();
        ThreadPoolTransformerFlag = false;
        return result;
    }

    /**
     *  DQL是不用开启事务的
     * @param targets 要查询的目标数据的主键id
     * @return 目标类型的List集合
     */
    public List<T> queryAll(List<Integer> targets){
         if (targets.size()==0)return null;

        return  targets.stream().map(a -> (Callable<T>) () -> queryById(a)).map(fixed::submit).map(future -> {
            try {
                return future.get();
            } catch (InterruptedException | ExecutionException e) {
                return null;
            }
        }).collect(Collectors.toList());

    }
    /**
     *
     * @param targets 要插入的目标对象的 List
     * @return 返回对应的执行情况，1是成功，0是失败，但如果失败会抛出异常的。
     */

    public List<Integer> insertAll(List<T> targets){
         if (targets.size()==0)return null;
        ThreadPoolTransformerFlag = true;
        List<Integer> result = targets.stream().map(a -> (Callable<Integer>) () -> insert(a)).map(fixed::submit).map(a -> {
            try {
                return  a.get();
            } catch (InterruptedException | ExecutionException e) {
                rollbackAll();
                throw new RuntimeException("出错");
            }
        }).collect(Collectors.toList());
        commitAll();
        ThreadPoolTransformerFlag =false;
        fixed.shutdown();
        return result;
    }

    /**
     *
     * @param rootFiledValues 要删除的记录的主键id组成的list
     * @return 对应的执行情况，1成功，0不存在
     */
    public List<Integer> deleteAll(List<Integer> rootFiledValues){
         if (rootFiledValues.size()==0)return null;
         ThreadPoolTransformerFlag = true;
        List<Integer> result = rootFiledValues.stream().map(a -> (Callable<Integer>) () -> delete(a)).map(fixed::submit).map(a -> {
            try {
                return a.get();
            } catch (InterruptedException | ExecutionException e) {
                rollbackAll();
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        ThreadPoolTransformerFlag = false;
        commitAll();
        fixed.shutdown();
        return result;

    }

    /**
     * 查找对应表中所有数据
     * @return 返回表中所有数据
     */
    public  List<T> getAll(){
          List<T> k;

        try {
            k = (List<T>) execute(OPERATION.SEL_ALL, K.DQL, tClass.newInstance());


        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return k;
    }

    /**
     *
     * @param value 目标记录的主键id
     * @return 查询到的目标记录
     */
    public  T queryById(long value){
        T grande;
        try {
            Field rootFiledT = Arrays.stream(tClass.getDeclaredFields()).filter(a -> a.getAnnotationsByType(PrimaryKey.class).length == 1).findFirst().orElse(null);
            T t = tClass.newInstance();
            Arrays.stream(tClass.getDeclaredFields()).filter(a-> a.equals(rootFiledT)).forEach(a-> {
                try {
                    a.setAccessible(true);
                    a.set(t,value);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            });
            List<T> result = (List<T>) execute( OPERATION.SEL, K.DQL, t);
            grande = result.stream().findFirst().orElse(null);

        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return grande;
    }

    /**
     *
     * @param value 目标记录的主键id
     * @return 是否删除成功 1 or 0
     */
    public  int delete(long value){
        T t;
        try {
            t = tClass.newInstance();
            Arrays.stream(tClass.getDeclaredFields()).filter(a-> a.equals(rootFiled)).forEach(a-> {
                try {
                    a.setAccessible(true);
                    a.set(t,value);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
      return Integer.parseInt( String.valueOf(execute(OPERATION.DEL,K.DML,t)));
    }

    public int update (T target){
        return Integer.parseInt(String.valueOf(execute(OPERATION.UPD, K.DML, target)));
    }
    public int insert(T target){
        return Integer.parseInt(String.valueOf(execute(OPERATION.INS,K.DML,target)));
    }

    /**
     * 最核心的执行SQL语句的部分，SQL的拼接，执行，返回结果都在这里
     * @param tag 执行的SQL的类型
     * @param flag 确定StringJoiner的类型
     * @param target 目标对象
     * @return  根据执行的操作不同，返回的结果也不尽相同。
     */
    public  Object execute(OPERATION tag, K flag, T target){

        PreparedStatement statement;
        Wrapper wrapper = (Wrapper) SQLHandler( null,null);
        Field[] fields = wrapper.fields;
        Object[] arg = new Object[flag.equals(K.DQL)?1:fields.length];
        int rootIndex = 0;
        ResultSet result;
        String info =  String.valueOf(SQLHandler(flag, tag));
        String sql=null;
        List<T> collect = new ArrayList<>();
        List<Map<String,Object>> all = new ArrayList<>();

        try {

           switch (flag){
               case DML:
                   switch (tag){
                       case UPD:
                           for (int i = 0,j=0; i < fields.length ; i++) {
                               if (fields[i].equals(rootFiled)){
                                   rootIndex=i;
                               }else {
                                   fields[i].setAccessible(true);
                                   arg[j++]=fields[i].get(target);

                               }
                           }
                           fields[rootIndex].setAccessible(true);
                           arg[arg.length-1]=fields[rootIndex].get(target);
                           sql= UPDATE+ table_Name +" set "+ info +WHERE+ rootFiledName+SEPARATED_END;break;
                       case DEL:
                           for (Field field : fields) {
                               if (field.equals(rootFiled)) {
                                   field.setAccessible(true);
                                   arg[0] = field.get(target);
                                   break;
                               }
                           }
                           sql= DELETE+ table_Name +WHERE+rootFiledName+SEPARATED_END;break;
                       case INS:
                           StringJoiner forIns = stringJoinerFactory(K.DQL);
                           for (int i = 0,j=0; i < fields.length ; i++) {
                                   fields[i].setAccessible(true);
                                   arg[j++]=fields[i].get(target);
                              forIns.add("?");
                           }
                           String string = forIns.toString();
                           sql= INSERT+ table_Name +"("+ info +")" +VALUES+"("+string+")";break;
                       default:
                   }break;
               case DQL:
                   if (tag.equals(OPERATION.SEL)){
                       for (Field field : fields) {
                           if (field.equals(rootFiled)) {
                               field.setAccessible(true);
                               arg[0] = field.get(target);
                               break;
                           }
                       }
                       sql =SELECT+info+FROM +table_Name+WHERE+rootFiledName+SEPARATED_END;
                   }else {

                       sql = SELECT+info+FROM +table_Name+";";
                   }

           }
        }catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        if (Optional.ofNullable(cacheForInstance.get(sql)).isPresent()){
            return cacheForInstance.get(sql);
        }
        try {

            statement = getConnection().prepareStatement(sql);
            for (int i = 0; i < arg.length; i++) {
               if (Optional.ofNullable(arg[i]).isPresent()){
                   Object temp = arg[i];
                   statement.setObject(i+1,temp);
               }else {
                   break;
               }
            }



            switch (flag){
                case DQL: result= statement.executeQuery();break;
                case DML: return statement.executeUpdate();
                default:result= null;
            }
            if (result!=null){
                ResultSetMetaData metaData = result.getMetaData();
                while (result.next()){
                    Map<String,Object> entry=new HashMap<>() ;
                    for(int i=1;i<=metaData.getColumnCount();i++) {
                        entry.put(metaData.getColumnLabel(i), result.getObject(i));
                    }
                    all.add(entry);
                }
                DBUtil.closeAll(null,statement,result);
                collect= all.stream().map(map -> GradeJSONImpl.getGrande(map, tClass)).collect(Collectors.toList());
                cacheForInstance.put(sql,collect);
            }else {
                DBUtil.closeAll(null, statement, null);
                cacheForInstance= new HashMap<>(32);
            }
            System.out.println(1);
            return collect;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 提交线程池中所有线程执行的操作
     */
    private void commitAll(){
         controlTransformer.forEach(a-> {
             try {
                 a.commit();
             } catch (SQLException e) {
                 throw new RuntimeException(e);
             }
         });
    }

    /**
     * 回滚线程池中所有线程执行的操作
     */
    private void rollbackAll(){
         controlTransformer.forEach(a-> {
             try {
                 a.rollback();
             } catch (SQLException e) {
                 throw new RuntimeException(e);
             }
         });
        System.out.println("已回滚");
    }

    /**
     * 根据K的类型确定所需要的StringJoiner的参数
     * @param m 确定StringJoiner
     * @return 所需参数的StringJoiner
     */
    @Contract("_ -> new")
    static @NotNull StringJoiner stringJoinerFactory(@NotNull K m){
            switch (m){
                case DQL:return new StringJoiner(SEPARATED_Q);
                case DML: return new StringJoiner(SEPARATED_M);
                default: throw new RuntimeException(EMPTY);
            }
    }

    /**
     * 为了确保参数拼接时的顺序，需要参数列表，有效的参数名
     */
    static class Wrapper{
        public Wrapper(Field[] fields) {
            this.fields = fields;
        }

        private final Field[] fields;


    }

    /**
     * 决定StringJoiner的参数
     */
    public enum K{
        DQL,DML
    }

    /**
     * 确定当前执行的是哪种SQL语句
     */
    public enum OPERATION {
        INS,DEL,UPD,SEL_ALL,SEL

    }

    /**
     * 自定义的线程工厂
     */
    public static class JDBCThreadFactory implements ThreadFactory{



        @Override
        public Thread newThread(@NotNull Runnable r) {
            Thread t = new Thread(r,"RPA"+a++);
            t.setUncaughtExceptionHandler((t1, e) -> e.printStackTrace());
            return t;
        }
    }

    /**
     * 将连接保存在线程中而非是对象里，确保不同线程操作对象情况下的一致性，
     * 并且如果是线程池中情况，会额外在对象本地保存一份连接，为了实现线程池
     * 中执行事务
     * @return  返回一个连接对象，如果线程之前已经获取过，从线程中取出，
     * 如果没有从DBUtils获取后，设置好自动提交，存入线程中，再返回connection
     */
    private Connection getConnection(){
        Connection connection = keepConnection.get();
        if (!Optional.ofNullable(connection).isPresent()) {
            connection = DBUtil.getConnection();
            if (!autoCommit){
                try {
                    connection.setAutoCommit(false);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            if (ThreadPoolTransformerFlag)
                controlTransformer.add(connection);
            keepConnection.set(connection);
        }
        return connection;
    }

    /**
     * 单线程情况下的默认提交方法。
     */
    public void commit(){
        if (autoCommit||!Optional.ofNullable(keepConnection.get()).isPresent())return;
        try {
            keepConnection.get().commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String INSERT = "insert into ";
    private static final String DELETE = "delete from ";
    private static final String UPDATE = "update ";
    private static final String SELECT = "select ";
    private static final String WHERE  = " where ";
    private static final String VALUES = " values ";
    private static final String EMPTY  = "";
    private static final String SEPARATED_M = " = ? , ";
    private static final String SEPARATED_Q = " , ";
    private static final String SEPARATED_END = " = ? ;";

    private static final String FROM = " from ";

    private final Class<T> tClass;
    private final String table_Name;

    private final Field rootFiled;

    private final String rootFiledName;

    private final boolean autoCommit;
    private static int a = 0;

    /**
     * 基于对象开启的缓存，在执行DML操作后清空
     */
    private HashMap<String,Object> cacheForInstance;
    /**
     * 开启线程池事务的标志
     */
    private boolean ThreadPoolTransformerFlag =false;

    /**
     * 本地中用于保存连接对象，实现事务
     */
    private final List<Connection> controlTransformer =new ArrayList<>(16);
    /**
     * 定长线程池，确保不会获取过多的连接，并且开启一次后线程一直存在，连接也一直保存在线程中
     */
    private final ThreadPoolExecutor fixed = new ThreadPoolExecutor(5, 5, 100, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(5), new JDBCThreadFactory());

    /**
     * 使用ThreadLocal将连接保存在线程中
     */
    private  final ThreadLocal<Connection> keepConnection = new ThreadLocal<>();

    /**
     * 获取所需的属性，会检测类中是否有需要的注解
     * param clazz 目标类
     * @param autoCommit 是否自动提交
     */
    public JDBCUtils (Class<T> clazz,boolean autoCommit){
        tClass = clazz;
        table_Name = clazz.getDeclaredAnnotation(Support.class).value().length()==0?clazz.getSimpleName():clazz.getDeclaredAnnotation(Support.class).value();
        rootFiled = Arrays.stream(tClass.getDeclaredFields()).filter(a -> a.getAnnotationsByType(PrimaryKey.class).length == 1).findFirst().orElseThrow(()->new RuntimeException("必须存在一个主键"));
        rootFiledName = rootFiled.getName();
        this.autoCommit = autoCommit;
        cacheForInstance = new HashMap<>(32);
    }

    /**
     * 默认开启自动提交
     * @param clazz 目标类型
     */
    public JDBCUtils(Class<T> clazz){
        this(clazz,true);
    }


}



