using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using Xunit;

namespace IntegrationTests;


public class Api : IClassFixture<PluggableContainer>
{
    private readonly PluggableContainer _pluggableContainer;

    public Api(PluggableContainer pluggableContainer)
    {
      _pluggableContainer = pluggableContainer;
      _pluggableContainer.SetBaseAddress();
    }

    [Fact]
    public async Task DaprHealthCheckIsSuccessful()
    {
      //Given
      const string path = "v1.0/metadata";

      // When
      var response = await _pluggableContainer.GetAsync(path)
        .ConfigureAwait(false);

      // var weatherForecastStream = await response.Content.ReadAsStreamAsync()
      //   .ConfigureAwait(false);

      // var weatherForecast = await JsonSerializer.DeserializeAsync<IEnumerable<WeatherData>>(weatherForecastStream)
      //   .ConfigureAwait(false);

      // // Then
      Assert.Equal(HttpStatusCode.OK, response.StatusCode);
      // Assert.Equal(7, weatherForecast!.Count());
    }
}

  // public sealed class Web : IClassFixture<DaprContainer>
  // {
  //   //private static readonly ChromeOptions ChromeOptions = new();

  //   private readonly DaprContainer _weatherForecastContainer;

  //   static Web()
  //   {
  //    // ChromeOptions.AddArgument("headless");
  //   //  ChromeOptions.AddArgument("ignore-certificate-errors");
  //   }

  //   public Web(DaprContainer weatherForecastContainer)
  //   {
  //     _weatherForecastContainer = weatherForecastContainer;
  //     _weatherForecastContainer.SetBaseAddress();
  //   }

  //   [Fact]
  //   [Trait("Category", nameof(Web))]
  //   public void Get_WeatherForecast_ReturnsSevenDays()
  //   {
  //     // Given
  //     string ScreenshotFileName() => $"{nameof(Get_WeatherForecast_ReturnsSevenDays)}_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}.png";

  //     using var chrome = new ChromeDriver(ChromeOptions);

  //     // When
  //     chrome.Navigate().GoToUrl(_weatherForecastContainer.BaseAddress);

  //     chrome.GetScreenshot().SaveAsFile(Path.Combine(CommonDirectoryPath.GetSolutionDirectory().DirectoryPath, ScreenshotFileName()));

  //     chrome.FindElement(By.TagName("fluent-button")).Click();

  //     var wait = new WebDriverWait(chrome, TimeSpan.FromSeconds(10));
  //     wait.Until(webDriver => 1.Equals(webDriver.FindElements(By.TagName("span")).Count));

  //     chrome.GetScreenshot().SaveAsFile(Path.Combine(CommonDirectoryPath.GetSolutionDirectory().DirectoryPath, ScreenshotFileName()));

  //     // Then
  //     Assert.Equal(7, int.Parse(chrome.FindElement(By.TagName("span")).Text, NumberStyles.Integer, CultureInfo.InvariantCulture));
  //   }
  // }
