import org.apache.commons.lang3.RandomUtils;

public class Test02Thread {
    public static void main(String[] args) {
        for (int i = 0;i<3;i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true){
                        System.out.println("hello");
                        try {
                            Thread.sleep(RandomUtils.nextInt(5000,6000));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }

    }
}
