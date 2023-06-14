import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.util.Date;


public class Demo {
    @Test
    public void t1() {
        String birthday = "1986-10-15";

        LocalDate now = LocalDate.now();
        System.out.println(now);

        LocalDate birth = LocalDate.parse(birthday);
        System.out.println(birth);

        Period between = Period.between(birth, now);
        System.out.println(between.getYears());
    }

    @Test
    public void t2() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH");
        Date date = new Date(1684505093000L);
        String dateStr = df.format(date);
        System.out.println(dateStr);
    }

    @Test
    public void t3() {
        LocalDate now = LocalDate.now();
        LocalDate yesterday = now.minusDays(1);
        System.out.println(yesterday);

    }
}
