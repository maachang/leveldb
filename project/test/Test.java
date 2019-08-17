package org.maachang.leveldb;

// 重複スレッドでの書き込みテスト.
//
public class Test {
    protected volatile boolean startFlag = false ;
    protected int endCount = 0 ;
    protected LevelMap map ;
    
    public static final void main( String[] args ) throws Exception {
        if( args == null || args.length == 0 ) {
            System.out.println( "error-setFolder?" ) ;
            return ;
        }
        int threadLen = 10 ;
        int addLength = 100000 ;
        
        Test t = new Test() ;
        LeveldbFactory.getInstance().init( args[ 0 ] ) ;
        
        long tm = System.currentTimeMillis() ;
        t.map = LeveldbFactory.getInstance().get( "test" ) ;
        
        TestThread[] tt = new TestThread[ threadLen ] ;
        for( int i = 0 ; i < threadLen ; i ++ ) {
            tt[ i ] = new TestThread( t,i * addLength,addLength ) ;
        }
        t.startFlag = true ;
        
        while( true ) {
            synchronized( t ) {
                if( t.endCount == threadLen ) {
                    break ;
                }
            }
            try { Thread.sleep( 100 ) ; } catch( Exception e ) {}
        }
        System.out.println( "## allExit" ) ;
        System.out.println( "length:" + t.map.size() + " " + ( System.currentTimeMillis() - tm ) + "msec" ) ;
        
        LeveldbFactory.getInstance().closeAll() ;
        
    }
    
    
    static final class TestThread extends Thread {
        private Test test ;
        private int count ;
        private int len ;
        
        public TestThread( Test t,int c,int l ) {
            test = t ;
            count = c ;
            len = l ;
            this.setDaemon( true ) ;
            this.start();
        }
        
        public void run() {
            while( !test.startFlag ) {
                try { Thread.sleep( 10 ) ; } catch( Exception e ) {}
            }
            long tm = System.currentTimeMillis() ;
            System.out.println( "## start:" + this.getId() ) ;
            
            int execLen = len ;
            int off = count ;
            String s ;
            LevelMap map = test.map ;
            for( int i = 0 ; i < execLen ; i ++ ) {
                s = String.valueOf( off + i ) ;
                map.put( s,off + i ) ;
            }
            System.out.println( "## exit:" + this.getId() + " - " + ( System.currentTimeMillis() - tm ) + "msec" ) ;
            synchronized( test ) {
                test.endCount ++ ;
            }
        }
    }
}
